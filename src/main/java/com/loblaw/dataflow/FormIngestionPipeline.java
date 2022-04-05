package com.loblaw.dataflow;


import avro.shaded.com.google.common.collect.ImmutableList;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.core.ApiFuture;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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

    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};
    public static void run(MyOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

        PubsubClient.SubscriptionPath path = PubsubClient.subscriptionPathFromName("my-demo-project-341408",
                "demo_topic-sub");
        PCollection<PubsubMessage> pCollection = pipeline.apply("Read PubSub messages",
                PubsubIO.readMessagesWithAttributes().fromSubscription(path.getPath()));
        pCollection.apply(ParDo.of(new CustomFn(options)))
                .apply(ParDo.of(new CustomTest()));
//                        .apply(
//                                "WriteSuccessfulRecords",
//                                BigQueryIO.writeTableRows()
//                                        .withoutValidation()
//                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                                        .withExtendedErrorInfo()
//                                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
//                                        .to(getTable(options.getProject(), "form_ingestion", "medCheck")));
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
        run(options);
    }
}

