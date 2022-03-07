package org.ascension.addg.gcp.pubsub;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONArray;
import org.json.JSONObject;

public class UpdateStatefileTimestamp extends DoFn<String, String>{
	
	private String timestamp;
	
	private String timestampKey;

	private PCollectionView<? extends Object> dependentPC;

	public UpdateStatefileTimestamp(String timestamp, String timestampKey, PCollectionView<? extends Object> dependentPC) {
		this.timestamp = timestamp;
		this.dependentPC = dependentPC;
		this.timestampKey = timestampKey;
	}

		@ProcessElement
		public void ProcessElement(ProcessContext c) throws IOException {
			
			JSONArray arr = new JSONArray(c.element());
			arr.length()
			final JSONObject jsonObj = (JSONObject) arr.get(0);
			//JSONObject jsonObj = new JSONObject(c.element());
			Set<String> keys = jsonObj.keySet();
			List<String> jsonKeyList = (List<String>) keys.stream().collect(Collectors.toList());
			List<? extends Object> li = (List<? extends Object>) c.sideInput(dependentPC);
			JSONObject newObj = new JSONObject();
			
			jsonKeyList.stream().forEach((k)->{
				if (k.equals(timestampKey)) {
					newObj.put(k, timestamp);
				}else {
					newObj.put(k, jsonObj.get(k));
				}
			});
			
			keys.stream().forEach((k)->{
				if(k.equals(timestampKey)) {
					jsonObj.put(k, timestamp);
				}
			});
			JSONArray newArr = new JSONArray();
	        newArr.put(newObj);
	        
	        String output = newArr.toString();
	        
	        //needs to be added in main class
	        //FileSystems.delete(List.of(FileSystems.matchNewResource(gcsPath, false)), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
			
	        c.output(String.valueOf(newArr));
		}
		
		public static void callUpdateTimestamp(PCollection<String> input, String time, String timestampKey, String gcsFileName, PCollectionView<? extends Object> objPC){
			input.apply(ParDo.of(new UpdateStatefileTimestamp(time, timestampKey, objPC)))
					.apply("WriteToStateFile", TextIO.write().to(gcsFileName).withoutSharding());
		}
}
