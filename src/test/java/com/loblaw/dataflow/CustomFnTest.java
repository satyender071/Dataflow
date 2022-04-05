package com.loblaw.dataflow;

import com.google.protobuf.ByteString;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CustomFnTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();



    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void check_pipeLine_should_run_correctly() throws Exception {

        FormIngestionPipeline.MyOptions myOptions = PipelineOptionsFactory
                .fromArgs("--projectId=test","--keyRing=test",
                        "--keyId=testKey", "--locationId=global",
                        "--bucketName=testBucket")
                .withValidation()
                .as(FormIngestionPipeline.MyOptions.class);
        DoFn.ProcessContext processContext = Mockito.mock(DoFn.ProcessContext.class);
        ByteString data = Mockito.mock(ByteString.class);
        Mockito.when((processContext.element())).thenReturn(data);
        Assertions.assertAll(() -> new FormIngestionPipeline.CustomFn(myOptions));
    }

}
