package streaming.test;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;

public class publish {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		
		Pipeline pipeline= Pipeline.create(options);
		
		HashMap<String, String> mapData = new HashMap<String, String>();
		JSONObject jsonData = new JSONObject();
		jsonData.put("query", "SELECT * FROM `sodium-chalice-334905.testdataset.players`");
		//String qu = "SELECT * FROM `sodium-chalice-334905.testdataset.players`";
		byte[] data = null;
		try {
			data = jsonData.toString().getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			
		}
		PubsubMessage msgnew = new PubsubMessage(data, mapData);
		
		PCollection<PubsubMessage> mj = pipeline.apply(Create.of(msgnew));
		mj.apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		pipeline.run();
	}

}
