package com.loblaw.dataflow;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;

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
}

