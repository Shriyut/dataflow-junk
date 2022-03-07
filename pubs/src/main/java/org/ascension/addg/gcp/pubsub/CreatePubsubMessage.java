package org.ascension.addg.gcp.pubsub;

import com.google.api.services.bigquery.model.TableRow;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.json.simple.JSONObject;

public class CreatePubsubMessage extends DoFn<TableRow, PubsubMessage>{
	
	//Class to convert each Tablerow record to PubsubMessage
	
	//Source system name is passed to the constructor of the class
	private String watermark;

	public CreatePubsubMessage(String watermark) {
		this.watermark = watermark;
	}
	
	
	HashMap<String, String> mapData = new HashMap<String, String>();
	//HashMap<String, String> mapData;
	JSONObject jsonData = new JSONObject();
	
	
	@ProcessElement
	public void ProcessElement(ProcessContext c)  {
		HashMap<String, String> mapData = new HashMap<String, String>();
		//HashMap<String, String> mapData;
		JSONObject jsonData = new JSONObject();
		TableRow clonedValue = c.element();
		clonedValue.set("SourceSystemName", watermark);
		clonedValue.entrySet().forEach(
				(k) -> {
					mapData.put(k.getKey(), String.valueOf(k.getValue()));
					jsonData.put(k.getKey(), k.getValue());
					
				});
		byte[] data = null;
		try {
			data = jsonData.toString().getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			
		}
		PubsubMessage msg = new PubsubMessage(data, mapData);
		c.output(msg);
	}
	

}
