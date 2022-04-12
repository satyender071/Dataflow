package com.loblaw.dataflow;


import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

@Slf4j
public class FormIngestionPipeline {

    public static void run(PubSubToBigQueryOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

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

        PipelineOptionsFactory.register(PubSubToBigQueryOptions.class);
        PubSubToBigQueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        run(options);
    }
}

