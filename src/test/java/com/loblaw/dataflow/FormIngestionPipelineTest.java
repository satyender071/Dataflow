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

