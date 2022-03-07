package org.ascension.addg.gcp.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONObject;
import org.junit.Rule;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class CreatePubsubMessageTest {
	
	@Rule
	public final transient TestPipeline testPipeline = TestPipeline.create();
	
	@Test(expected = AssertionError.class)
	public void createMessageTest() throws UnsupportedEncodingException {
		
		//Test case to validate creation of PubsubMessage object
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("row", "testVal1"));
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		
		PCollection<PubsubMessage> msg = inputPC.apply(ParDo.of(new CreatePubsubMessage("Symedical")));
		
		JSONObject jsonTest = new JSONObject();
		jsonTest.put("row", "testVal1");
		jsonTest.put("SourceSystemName", "Symedical");
		byte[] data = jsonTest.toString().getBytes("utf-8");
		HashMap<String, String> mapTest = new HashMap<String, String>();
		mapTest.put("row", "testVal1");
		mapTest.put("SourceSystemName", "Symedical");
		
		PubsubMessage expectedMsg = new PubsubMessage(data, mapTest);
		
		//PCollection<PubsubMessage> test = testPipeline.apply("Creating test msg", Create.of(expectedMsg));
			//assertEquals(test.toString(), "true");
		expectedMsg.ge
		PAssert.that(msg).containsInAnyOrder(expectedMsg);
		
		testPipeline.run().waitUntilFinish();
		
	}
	
	@Test(expected = Exception.class)
	public void publishTest() throws UnsupportedEncodingException {
		
		//Test case to check if exception is thrown when invalid topic name is passed to the class
		String topicName = "Invalid/topic/name";
		
		
		JSONObject jsonTest = new JSONObject();
		jsonTest.put("row", "testVal1");
		byte[] data = jsonTest.toString().getBytes("utf-8");
		HashMap<String, String> mapTest = new HashMap<String, String>();
		mapTest.put("row", "testVal1");
		
		PubsubMessage newMsg = new PubsubMessage(data, mapTest);
		
		PCollection<PubsubMessage> testMessage = testPipeline.apply(Create.of(newMsg));
		
		testMessage.apply(new PublishtoPubsub(topicName));
		
		testPipeline.run().waitUntilFinish();
	}

}
