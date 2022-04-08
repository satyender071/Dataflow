package com.loblaw.dataflow;


import avro.shaded.com.google.common.collect.ImmutableList;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.core.ApiFuture;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

@Slf4j
public class FormIngestionPipeline {

    public interface MyOptions extends PipelineOptions, StreamingOptions, GcsOptions, DataflowPipelineOptions {

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
        pCollection.apply(ParDo.of(new CustomFn(options)))
//        PCollectionTuple tupleResult = message.apply("TransformToBQ", TransformToBQ.run());
//        WriteResult writeResult = tupleResult.get(TransformToBQ.SUCCESS_TAG)
//                .apply("test",BigQueryIO.writeTableRows()
//                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
//                        .withWriteDisposition(WRITE_APPEND)
//                        .withCreateDisposition(CREATE_NEVER)
//                        .withExtendedErrorInfo() //- getFailedInsertsWithErr
//                        .ignoreUnknownValues()
//                        .skipInvalidRows()
//                        .withoutValidation()
//                        .to((row) -> {
//                            String tableName = "userFormData3";
//                            return new TableDestination(String.format("%s:%s.%s", "my-demo-project-341408", "form_ingestion", tableName), "Some destination");
//                        }));
//        writeResult.getFailedInsertsWithErr()
//                .apply("MapFailedInserts", MapElements.via(new SimpleFunction<BigQueryInsertError, KV<String, String>>() {
//                                                               @Override
//                                                               public KV<String, String> apply(BigQueryInsertError input) {
//                                                                   return KV.of("FailedInserts", input.getError().toString() + " for table" + input.getRow().get("table") + ", message: "+ input.getRow().toString());
//                                                               }
//                                                           }
//                ));

                .apply(ParDo.of(new CustomTest()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    static TableReference getTable(String projectId, String datasetId, String tableName) {
        TableReference table = new TableReference();
        table.setDatasetId(datasetId);
        table.setProjectId(projectId);
        table.setTableId(tableName);
        return table;
    }
    public static void main(String[] args) throws IOException {

        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        options.setStreaming(true);
        options.setJobName("formData_cloud_storage_job");
        run(options);
    }
}

