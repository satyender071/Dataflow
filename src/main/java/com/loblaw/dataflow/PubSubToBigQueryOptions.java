package com.loblaw.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.*;

public interface PubSubToBigQueryOptions extends DataflowPipelineOptions {

    @Description("GCP KeyRing name")
    ValueProvider<String> getKeyRing();

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
