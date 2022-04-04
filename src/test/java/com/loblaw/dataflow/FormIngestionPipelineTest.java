package com.loblaw.dataflow;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FormIngestionPipelineTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @InjectMocks
    FormIngestionPipeline formIngestionPipeline;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPubsubIOGetName() {
        assertEquals("PubsubIO.Read",
                PubsubIO.readStrings().fromTopic("projects/myproject/topics/mytopic").getName());
        assertEquals("PubsubIO.Write",
                PubsubIO.writeStrings().to("projects/myproject/topics/mytopic").getName());
    }

    @Test
    public void testTopicValidationBadCharacter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        PubsubIO.readStrings().fromTopic("projects/my-project/topics/abc-*-abc");
    }

    @Test
    public void fileName_should_start_with_given_fileName_parameter() throws Exception {
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
        assertTrue(fileName, formIngestionPipeline.fileName("ABC").startsWith("ABC"));
    }

    @Test
    public void folderName_should_be_of_correct_month() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int month = calendar.get(Calendar.MONTH) + 1;
        Integer year = calendar.get(Calendar.YEAR);
        String date = year + "-" + (month < 10 ? ("0" + month) : (month));
        assertEquals(date + "/", formIngestionPipeline.folderName());
    }


    @Test
    public void test_projectId_with_default() {
        FormIngestionPipeline.MyOptions options = PipelineOptionsFactory
                .fromArgs("--projectId=test","--keyRing=test",
                        "--keyId=testKey", "--locationId=global",
                        "--bucketName=testBucket")
                .as(FormIngestionPipeline.MyOptions.class);
        ValueProvider<String> projectId = options.getProjectId();
        ValueProvider<String> keyRing = options.getKeyRing();
        ValueProvider<String> keyId = options.getKeyId();
        ValueProvider<String> locationId = options.getLocationId();
        ValueProvider<String> bucketName = options.getBucketName();
        assertEquals("test", projectId.get());
        assertEquals("test", keyRing.get());
        assertEquals("testKey", keyId.get());
        assertEquals("global", locationId.get());
        assertEquals("testBucket", bucketName.get());
        assertTrue(projectId.isAccessible());
        assertTrue(keyRing.isAccessible());
        assertTrue(keyId.isAccessible());
        assertTrue(locationId.isAccessible());
        assertTrue(bucketName.isAccessible());
    }

}

