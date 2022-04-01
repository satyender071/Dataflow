package com.loblaw.dataflow;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.core.ApiFuture;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@Slf4j
public class PipelineDecryptionService {

    public static void run(PipelineOptions options) throws IOException {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<PubsubMessage> pCollection = pipeline.apply("Read PubSub messages",
                PubsubIO.readMessagesWithAttributes().
                        fromSubscription("projects/my-demo-project-341408/subscriptions/demo_topic-sub"));
//                .apply("write to topic", PubsubIO.writeMessages().to("projects/my-demo-project-341408/topics/test_topic"));
        pCollection.apply(ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void process(ProcessContext context) throws IOException, ExecutionException, InterruptedException {
                        DecryptResponse decrypt = null;
                        Publisher publisher = null;
                        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
                            ByteString data = ByteString.copyFrom(Objects.requireNonNull(context.element()).getPayload());
                            CryptoKeyName keyVersionName = CryptoKeyName.of("my-demo-project-341408",
                                    "global", "my-asymmetric-signing-key1", "my-new-key-id");
                            decrypt = client.decrypt(keyVersionName, data);
                            log.info("Decrypted Data: {}", decrypt.getPlaintext().toStringUtf8());
                            JSONObject jsonObject = new JSONObject(decrypt.getPlaintext().toStringUtf8());
                            JSONObject object = jsonObject.getJSONObject("formMetaData");
                            JSONObject test = new JSONObject(jsonObject.getJSONObject("formData").get("formData").toString());
                            String pureJson = JsonUnflattener.unflatten(test.toString());
                            JSONObject pureJsonFormat = new JSONObject(pureJson);
                            log.info("json data: {}", pureJsonFormat);

                    publisher = Publisher.newBuilder("projects/my-demo-project-341408/topics/test_topic").build();
                    com.google.pubsub.v1.PubsubMessage pubsubMessage = com.google.pubsub.v1.PubsubMessage
                            .newBuilder().setData(ByteString.copyFromUtf8(pureJsonFormat.toString())).build();
                    ApiFuture<String> publishedMessage = publisher.publish(pubsubMessage);
                    log.info("Message id generated:{}", publishedMessage.get());
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            if (publisher != null) {
                                publisher.shutdown();
                                publisher.awaitTermination(30, TimeUnit.SECONDS);
                            }
                        }
                    }
                })).apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
                .apply("Write to GCS", TextIO.write().withWindowedWrites()
                        .withNumShards(1)
                        .to("gs://demo_bucket_topic")
                        .withSuffix(".txt"));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish(Duration.standardMinutes(1));
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
//        options.setStreaming(true);
        run(options);
    }

}

