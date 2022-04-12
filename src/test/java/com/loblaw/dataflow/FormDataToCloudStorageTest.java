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

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FormDataToCloudStorageTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void check_pipeLine_should_run_correctly() throws Exception {

        PubSubToBigQueryOptions myOptions = PipelineOptionsFactory
                .fromArgs("--projectId=test","--keyRing=test",
                        "--keyId=testKey", "--locationId=global",
                        "--bucketName=testBucket")
                .withValidation()
                .as(PubSubToBigQueryOptions.class);
        DoFn.ProcessContext processContext = Mockito.mock(DoFn.ProcessContext.class);
        ByteString data = Mockito.mock(ByteString.class);
        Mockito.when((processContext.element())).thenReturn(data);
        Assertions.assertAll(() -> new FormDataToCloudStorageFn(myOptions));
    }

    @Test
    public void fileName_should_start_with_given_fileName_parameter() throws Exception {

        PubSubToBigQueryOptions myOptions = PipelineOptionsFactory
                .fromArgs("--projectId=test","--keyRing=test",
                        "--keyId=testKey", "--locationId=global",
                        "--bucketName=testBucket")
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        String fileName;
        String formName = "ABC";
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        int seconds = calendar.get(Calendar.SECOND);
        SecureRandom secureRandom = new SecureRandom();
        fileName = formName + hours + "_" + minutes +
                "_" + seconds + "_";
        FormDataToCloudStorageFn customFn = new FormDataToCloudStorageFn(myOptions);
        assertTrue(fileName, customFn.fileName("ABC").startsWith("ABC"));
    }

    @Test
    public void folderName_should_be_of_correct_month() throws Exception {

        PubSubToBigQueryOptions myOptions = PipelineOptionsFactory
                .fromArgs("--projectId=test","--keyRing=test",
                        "--keyId=testKey", "--locationId=global",
                        "--bucketName=testBucket")
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int month = calendar.get(Calendar.MONTH) + 1;
        Integer year = calendar.get(Calendar.YEAR);
        String date = year + "-" + (month < 10 ? ("0" + month) : (month));
        FormDataToCloudStorageFn customFn = new FormDataToCloudStorageFn(myOptions);
        assertEquals(date + "/", customFn.folderName());
    }

}
