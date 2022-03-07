package streaming.test;

import static org.apache.beam.sdk.transforms.windowing.AfterProcessingTime.pastFirstElementInPane;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.simple.JSONObject;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class CheckNew {

	public static final int ALLOWED_LATENESS_SEC = 0;
    public static final int SESSION_WINDOW_GAP_DURATION = 10;
    
	public static void main(String[] args) {
		
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		
		Pipeline pipeline= Pipeline.create(options);
		
		Trigger trigger = Repeatedly.forever(pastFirstElementInPane()
                .orFinally(AfterWatermark.pastEndOfWindow()));
		
		Window<PubsubMessage> sessionWindow =
                Window.<PubsubMessage>into(Sessions.withGapDuration(
                        Duration.standardSeconds(SESSION_WINDOW_GAP_DURATION)))
                        .triggering(trigger)
                        .withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_SEC))
                        .accumulatingFiredPanes();
		PCollection<TableRow> rows = pipeline.apply(BigQueryIO.readTableRows()
				.fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").usingStandardSql());
		
		PCollection<PubsubMessage> test = pipeline.apply(PubsubIO.readMessages().fromSubscription("projects/sodium-chalice-334905/subscriptions/sampletopic-sub"));
		//PCollection<PubsubMessage> triggerMsg = test.apply(new ProcessMsg(pipeline));
		PCollection<PubsubMessage> result = test.apply("Applying sessionwindow", sessionWindow)
				.apply(new ProcessMsg(pipeline));
		
		result.apply("Applying sessionwindow", sessionWindow).apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		
		pipeline.run();
	}
	
	
	public static class ProcessMsg extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>>{
		
		private Pipeline pipeline;

		public ProcessMsg(Pipeline pipleine) {
			this.pipeline = pipeline;
		}

		@Override
		public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
			
			PCollection<PubsubMessage> op = input.apply(ParDo.of(new msgFn(pipeline)));
			return op;
		}
		
	}
	
	public static class msgFn extends DoFn<PubsubMessage, PubsubMessage>{
		
		private Pipeline pipeline;
		
		public msgFn(Pipeline pipeline) {
			this.pipeline = pipeline;
			
		}
		@ProcessElement
		public void ProcessElement(ProcessContext ctx) {
			PubsubMessage msg = ctx.element();
			byte[] val = msg.getPayload();
			Map<String, String> msgAttributes = msg.getAttributeMap();
			System.out.println(String.valueOf(msgAttributes));
			try {
			String q = msgAttributes.get("query");
			}catch(NullPointerException e) {
				System.out.println("uiop");
			}
			String query = new String(val, StandardCharsets.UTF_8);
			
			System.out.println("query "+query);
			PCollection<TableRow> rows = null;
			try {
			rows = pipeline.apply(BigQueryIO.readTableRows()
					  .fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").usingStandardSql());
			}catch(NullPointerException e) {
				System.out.println("caught");
				e.printStackTrace();
			}
			
			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 
			fields.add(new TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("jerseynumber").setType("INT64").setMode("NULLABLE"));
			TableSchema schema = new TableSchema().setFields(fields);
			
			rows.apply("Writing to BQ", BigQueryIO.writeTableRows()
					.to("testdataset.playersneww")
					.withSchema(schema)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
			
			HashMap<String, String> mapData = new HashMap<String, String>();
			JSONObject jsonData = new JSONObject();
			//jsonData.put("SELECT * FROM `sodium-chalice-334905.testdataset.players`");
			mapData.put("query", "SELECT * FROM `sodium-chalice-334905.testdataset.players`");
			String qu = "SELECT * FROM `sodium-chalice-334905.testdataset.players`";
			byte[] data = null;
			try {
				data = qu.toString().getBytes("utf-8");
			} catch (UnsupportedEncodingException e) {
				
			}
			PubsubMessage msgnew = new PubsubMessage(data, mapData);
			//PCollection<PubsubMessage> msgPC = pipeline.apply(Create.of(msgnew));
			ctx.output(msgnew);
		}
	}

}
