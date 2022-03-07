package streaming.test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.simple.JSONObject;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

public class First {

	public static void main(String[] args) throws UnsupportedEncodingException {
		
		First firstObj = new First();
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		
		Pipeline pipeline= Pipeline.create(options);
		//read from pubsub code here
		
		PCollection<PubsubMessage> test = pipeline.apply(PubsubIO.readMessages().fromSubscription("projects/sodium-chalice-334905/subscriptions/sampletopic-sub")
				);
		//PCollection<PubsubMessage> newt = pipeline.apply(Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1))));
		//PCollection<PubsubMessage> returnmsg = newt.apply(new Test(pipeline));
		//returnmsg.apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		pipeline.run();
		//firstObj.run(pipeline, newt);
	}
	
	public void run(Pipeline pipeline, PCollection<PubsubMessage> initiation) throws UnsupportedEncodingException {
		//read and write to bq code here
		
		PCollection<TableRow> rows = pipeline.apply(BigQueryIO.readTableRows()
				  .fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").usingStandardSql());
		
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 
		fields.add(new TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("jerseynumber").setType("INT64").setMode("NULLABLE"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		rows.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("testdataset.playersnew")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		//publish to pubsub code here
		HashMap<String, String> mapData = new HashMap<String, String>();
		JSONObject jsonData = new JSONObject();
		jsonData.put("source", "MD-Staff");
		byte[] data = jsonData.toString().getBytes("utf-8");
		PubsubMessage msg = new PubsubMessage(data, mapData);
		PCollection<PubsubMessage> msgPC = pipeline.apply(Create.of(msg));
		msgPC.apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		
		pipeline.run();
	}

	public static class Test extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>>{

		private Pipeline pipeline;
		public Test(Pipeline pipeline) {
			this.pipeline = pipeline;
		}
		@Override
		public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
			
			PCollection<TableRow> rows = pipeline.apply(BigQueryIO.readTableRows()
					  .fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").usingStandardSql());
			
			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 
			fields.add(new TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("jerseynumber").setType("INT64").setMode("NULLABLE"));
			TableSchema schema = new TableSchema().setFields(fields);
			
			rows.apply("Writing to BQ", BigQueryIO.writeTableRows()
					.to("testdataset.playersnew")
					.withSchema(schema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
			//publish to pubsub code here
			HashMap<String, String> mapData = new HashMap<String, String>();
			JSONObject jsonData = new JSONObject();
			jsonData.put("source", "MD-Staff");
			byte[] data = null;
			try {
				data = jsonData.toString().getBytes("utf-8");
			} catch (UnsupportedEncodingException e) {
				
			}
			PubsubMessage msg = new PubsubMessage(data, mapData);
			PCollection<PubsubMessage> msgPC = pipeline.apply(Create.of(msg));
			
			//ImmutableList<TableRow> inputtr = ImmutableList.of(new TableRow().set("run_id", "testVal1").set("run_error", "testVal2"));
			//PCollection<TableRow> inputPC = pipeline.apply(Create.<TableRow>of(inputtr));
			
			return msgPC;
		}
		
	}
	
	
}
