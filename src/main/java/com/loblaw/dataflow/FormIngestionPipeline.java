package com.loblaw.dataflow;


import avro.shaded.com.google.common.collect.ImmutableList;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.*;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class FormIngestionPipeline {

    public interface MyOptions extends PipelineOptions, StreamingOptions, GcsOptions {

        @Description("GCP KeyRing name")
        @Default.String("my-asymmetric-signing-key1")
        ValueProvider<String>  getKeyRing();

        void setKeyRing(ValueProvider<String> keyRing);

        @Description("GCP Key Id name")
        @Default.String("my-new-key-id")
        ValueProvider<String> getKeyId();

        void setKeyId(ValueProvider<String> keyid);

        @Description("GCP Project Id name")
        @Default.String("my-demo-project-341408")
        ValueProvider<String> getProjectId();

        void setProjectId(ValueProvider<String> projectId);

        @Description("GCP Location Id name")
        @Default.String("global")
        ValueProvider<String> getLocationId();

        void setLocationId(ValueProvider<String> locationId);

        @Description("GCP BucketName")
        @Default.String("demo_bucket_topic")
        ValueProvider<String> getBucketName();

        void setBucketName(ValueProvider<String> bucketName);
    }

    public static void run(MyOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

        PubsubClient.SubscriptionPath path = PubsubClient.subscriptionPathFromName("my-demo-project-341408",
                "demo_topic-sub");
        PCollection<PubsubMessage> pCollection = pipeline.apply("Read PubSub messages",
                PubsubIO.readMessagesWithAttributes().fromSubscription(path.getPath()));
        pCollection.apply(ParDo.of(new CustomFn(options)));
        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    private static String fileName(String formName) {
        String fileName;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        int seconds = calendar.get(Calendar.SECOND);
        SecureRandom secureRandom = new SecureRandom();
        fileName = formName + hours + "_" + minutes +
                "_" + seconds + "_" + secureRandom.nextInt(9999) + "--";
        return fileName;
    }

    private static String folderName() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int month = calendar.get(Calendar.MONTH) + 1;
        Integer year = calendar.get(Calendar.YEAR);
        String date = year + "-" + (month < 10 ? ("0" + month) : (month));
        return date + "/";
    }

    static class CustomFn extends DoFn<PubsubMessage, String> {

        ValueProvider<String> projectId;
        ValueProvider<String> keyRing;
        ValueProvider<String> keyId;
        ValueProvider<String> location;
        ValueProvider<String> bucketName;
        public CustomFn(MyOptions options) {
            this.keyId = options.getKeyId();
            this.projectId = options.getProjectId();
            this.location = options.getLocationId();
            this.keyRing = options.getKeyRing();
            this.bucketName = options.getBucketName();
        }
        @ProcessElement
        public void process(ProcessContext context) {
            DecryptResponse decrypt = null;
            try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
                ByteString data = ByteString.copyFrom(Objects.requireNonNull(context.element()).getPayload());
                CryptoKeyName keyVersionName = CryptoKeyName.of(projectId.get(),
                        location.get(), keyRing.get(), keyId.get());
                decrypt = client.decrypt(keyVersionName, data);
                log.info("Decrypted Data: {}", decrypt.getPlaintext().toStringUtf8());
                JSONObject jsonObject = new JSONObject(decrypt.getPlaintext().toStringUtf8());
                JSONObject object = jsonObject.getJSONObject("formMetaData");

                JSONObject formData = new JSONObject(jsonObject.getJSONObject("formData").get("formData").toString());
//                JSONObject formData = new JSONObject(test.get("formData"));
                String pureJson = JsonUnflattener.unflatten(formData.toString());
                JSONObject pureJsonFormat = new JSONObject(pureJson);
                log.info("json data: {}", pureJsonFormat);

                String formName = String.valueOf(object.get("formName"));
                String fileName = fileName(formName);
                log.info("fileName is : {}", fileName);

                Storage storage = StorageOptions.newBuilder().
                        setProjectId(projectId.get()).build().getService();
                BlobId blobId = BlobId.of(Objects.requireNonNull(bucketName.get()), formName + "/"
                        + folderName() + fileName + ".json");
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                byte[] content = data.toByteArray();
                storage.createFrom(blobInfo, new ByteArrayInputStream(content));

                log.info("storage data: {}", content);

                BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

                DatasetInfo datasetInfo = DatasetInfo.newBuilder("form_ingestion").build();

                Dataset newDataset = bigquery.create(datasetInfo);
                String newDatasetName = newDataset.getDatasetId().getDataset();
                log.info(newDatasetName + " created successfully");

                TableId tableId = TableId.of("form_ingestion", formName);
                TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of());
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

                log.info("TableId : {}", tableId);
//                if( !tableId.getTable().equals(formName)) {

                    bigquery.create(tableInfo);

                    Schema newSchema =
                            Schema.of(
                                    Field.newBuilder("formData", LegacySQLTypeName.STRING)
                                            .setMode(Field.Mode.REQUIRED)
                                            .build());
                                    // Adding below additional column during the load job
//                                    Field.newBuilder("post_abbr", LegacySQLTypeName.STRING)
//                                            .setMode(Field.Mode.NULLABLE)
//                                            .build());

                    LoadJobConfiguration loadJobConfig =
                            LoadJobConfiguration.builder(tableId, pureJsonFormat.toString())
                                    .setFormatOptions(FormatOptions.csv())
                                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                                    .setSchema(newSchema)
                                    .setSchemaUpdateOptions(ImmutableList.of(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION))
                                    .build();

                    JobId jobId = JobId.of(UUID.randomUUID().toString());
                    Job loadJob = bigquery.create(JobInfo.newBuilder(loadJobConfig).setJobId(jobId).build());

                    // Load data from a GCS parquet file into the table
                    // Blocks until this load table job completes its execution, either failing or succeeding.
                    Job completedJob = loadJob.waitFor();
                    log.info("job details: {}", completedJob);
//                }

            } catch (IOException e) {
                log.error("Can not process the data successfully");
                e.printStackTrace();
            } catch (InterruptedException e) {
                log.error("Can not write data to bigquery table");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

//        options.setProjectId("my-demo-project-341408");
        options.setStreaming(true);
        run(options);
    }

}

