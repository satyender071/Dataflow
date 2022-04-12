package com.loblaw.dataflow;


import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

@Slf4j
public class FormIngestionPipeline {

    public interface MyOptions extends DataflowPipelineOptions {

        @Description("GCP KeyRing name")
        ValueProvider<String>  getKeyRing();

        void setKeyRing(ValueProvider<String> value);

        @Description("GCP Key Id name")
        ValueProvider<String> getKeyId();

        void setKeyId(ValueProvider<String> value);

        @Description("GCP Project Id name")
        ValueProvider<String> getProjectId();

        void setProjectId(ValueProvider<String> value);

        @Description("GCP Location Id name")
        ValueProvider<String> getLocationId();

        void setLocationId(ValueProvider<String> value);

        @Description("GCP BucketName")
        ValueProvider<String> getBucketName();

        void setBucketName(ValueProvider<String> value);

        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>")
        @Validation.Required
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description(
                "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

    }

    public static void run(MyOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

//        PubsubClient.SubscriptionPath path = PubsubClient.subscriptionPathFromName(options.getProjectId().get(),
//                "demo_topic-sub");
        PCollection<PubsubMessage> pCollection = pipeline.apply("Read PubSub messages",
                PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
        pCollection.apply(ParDo.of(new FormDataToCloudStorageFn(options)))
                .apply(ParDo.of(new FormDataToBigQuery(options)));

        PipelineResult result = pipeline.run();
        try {
            result.getState();
            result.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            // do nothing
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        run(options);
    }
}

