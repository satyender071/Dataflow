package com.loblaw.dataflow;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.api.services.bigquery.model.*;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class FormDataToBigQuery extends DoFn<String, TableRow> {

    ValueProvider<String> projectId;
    ValueProvider<String> keyRing;
    ValueProvider<String> keyId;
    ValueProvider<String> location;
    ValueProvider<String> bucketName;

    public FormDataToBigQuery(PubSubToBigQueryOptions options) {
        this.keyId = options.getKeyId();
        this.projectId = options.getProjectId();
        this.location = options.getLocationId();
        this.keyRing = options.getKeyRing();
        this.bucketName = options.getBucketName();
    }

    @ProcessElement
    public void processFormDataToBigQuery(ProcessContext context) throws IOException {

        log.info("data unFlattened: {}", context.element());
        JSONObject jsonObject = new JSONObject(Objects.requireNonNull(context.element()));

        TableRow row = convertJsonToTableRow(jsonObject.toString());
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table table = bigquery.getTable("form_ingestion", "userFormData2");
        Schema schema = table.getDefinition().getSchema();

        assert schema != null;
        FieldList fields = schema.getFields();

        List<Field> fieldList = new ArrayList<>(fields);
        log.info("My List size: {}", fieldList.size());
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        String result = JsonFlattener.flatten(jsonObject.toString());
        JSONObject unFlattened = new JSONObject(result);

        List<Field> newfieldList = new ArrayList<>();
        Map<String,String> keyValue = new HashMap<>();
        unFlattened.keys().forEachRemaining(key -> {
            String myKey = key;
            keyValue.put(myKey.replace('.','_'), unFlattened.get(key).toString());
            log.info("key : {} and  value: {}", myKey.replace('.','_'), unFlattened.get(key));
            newfieldList.add(Field.of(myKey.replace('.','_'), LegacySQLTypeName.STRING));
        });

        log.info("flattened key value pair: {}", keyValue.keySet());
        log.info("new filed list size:{}", newfieldList.size());
        Schema newSchema = Schema.of(newfieldList);
        log.info("fields: {}", newSchema.getFields());
        log.info("row is : {}", row);

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId.get()).build().getService();
        BlobId blobId = BlobId.of(Objects.requireNonNull("pure_json_bucket"),
                jsonObject.getJSONObject("formMetaData").get("formName") + "/" + "fileName" + ".json");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        byte[] content = jsonObject.toString().getBytes();
        storage.createFrom(blobInfo, new ByteArrayInputStream(content));

        try {
            TableId tableId = TableId.of("form_ingestion", "formData1");
//            Table updatedTable = table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
//            updatedTable.update();

            LoadJobConfiguration loadJobConfig =
                    LoadJobConfiguration.builder(tableId, "gs://pure_json_bucket/" + jsonObject.getJSONObject("formMetaData").get("formName") + "/"
                                    + "fileName" + ".json")
                            .setFormatOptions(FormatOptions.json())
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .setAutodetect(true)
                            .setSchemaUpdateOptions(ImmutableList.of(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
                            .build();

            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job job = bigquery.create(JobInfo.newBuilder(loadJobConfig).setJobId(jobId).build());
            job = job.waitFor();
            if (job.isDone()) {
                log.info("Json from GCS successfully loaded in a table");
            } else {
                log.info(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            log.info("Column not added during load append \n" + e.toString());
        }
        log.info("Hello I am here");
        context.output(row);

    }

    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }
}
