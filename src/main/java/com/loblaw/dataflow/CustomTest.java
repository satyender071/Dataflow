package com.loblaw.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Slf4j
public class CustomTest extends DoFn<String, TableRow> {

    @ProcessElement
    public void test(ProcessContext context) {
        log.info("data unflatenned: {}", context.element());
        JSONObject jsonObject = new JSONObject(Objects.requireNonNull(context.element()));

        TableRow row = convertJsonToTableRow(jsonObject.toString());
        List<TableFieldSchema> fields = new ArrayList<>();
        TableSchema schema;

        jsonObject.keys().forEachRemaining(key -> {

            log.info("key : {} and  value: {}", key,jsonObject.get(key));
//            row.set(key, jsonObject.get(key));
            fields.add(new TableFieldSchema().setName(key).setType("STRING"));
        });
        schema = new TableSchema().setFields(fields);
        log.info("fields: {}", schema.getFields());
        log.info("row is : {}", row);

//        BigQueryIO.writeTableRows()
//                .withoutValidation()
//                .withSchema(new TableSchema())
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withExtendedErrorInfo()
//                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
//                .to(getTable("my-demo-project-341408", "form_ingestion", "medCheck"));

        context.output(row);

    }

    public TableReference getTable(String projectId, String datasetId, String tableName) {
        TableReference table = new TableReference();
        table.setDatasetId(datasetId);
        table.setProjectId(projectId);
        table.setTableId(tableName);
        return table;
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
