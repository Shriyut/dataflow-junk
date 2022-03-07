package com.journaldev.jackson.json;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ProcessJsonNode {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		InputStream is = ProcessJsonNode.class.getResourceAsStream("Test.json");
		
		JsonNode jsonTree= new ObjectMapper().readTree(is);
		
		for(JsonNode node : jsonTree.get("riq_data")) {
			
			JsonNode attribute= node.get("attributes");
			System.out.println(attribute);
			processNode(attribute);
		}
		
		try {  

			new ObjectMapper().writeValue(new File("Test-1.json"), jsonTree );

	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  

	}
	
	
	private static void processNode(JsonNode jsonNode) throws InterruptedException {

		if (jsonNode.isValueNode()) {
		} else if (jsonNode.isArray()) {

			List<JsonNode> list = new ArrayList<>();
			for (JsonNode arrayItem : jsonNode) {
				if (arrayItem.has("isOv") && arrayItem.get("isOv").asBoolean()) {
					processNode(arrayItem);
					list.add(arrayItem);
				}
			}
			((ArrayNode) jsonNode).removeAll().addAll(list);
		} else if (jsonNode.isObject()) {
			ObjectNode objectNode = (ObjectNode) jsonNode;
			List<String> keysToRemove = new ArrayList<String>();

			Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
			while (iter.hasNext()) {
				Map.Entry<String, JsonNode> entry = iter.next();

				if (entry.getValue().isArray() && !entry.getValue().has(0))
					keysToRemove.add(entry.getKey());
				else {
					processNode(entry.getValue());
				}
			}
			objectNode.remove(keysToRemove);
		}
	}

}
