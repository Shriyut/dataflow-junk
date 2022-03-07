package streaming.test;

import static org.apache.beam.sdk.transforms.windowing.AfterProcessingTime.pastFirstElementInPane;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class Api {
	
	public static final int ALLOWED_LATENESS_SEC = 0;
	public static final int SESSION_WINDOW_GAP_DURATION = 10;


	public static void main(String[] args) {
		// TODO Auto-generated method stub

		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);
		options.setStreaming(true);
		Pipeline pipeline = Pipeline.create(options);
		
		Trigger trigger = Repeatedly.forever(pastFirstElementInPane().orFinally(AfterWatermark.pastEndOfWindow()));

		Window<PubsubMessage> sessionWindow = Window
				.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardSeconds(SESSION_WINDOW_GAP_DURATION)))
				.withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_SEC))
				.accumulatingFiredPanes();
		
		PCollection<PubsubMessage> test = pipeline.apply(PubsubIO.readMessages()
				.fromSubscription("projects/sodium-chalice-334905/subscriptions/sampletopic-sub"));
		//PCollection<PubsubMessage> h = test.apply(sessionWindow);
		PCollection<TableRow> write = test.apply(new PerformProcessing());
		//write.apply(sessionWindow);
		//PCollection<PubsubMessage> triggerMsg = write.apply(Wait.on(write)).apply(ParDo.of(new CreateMdstaffPubsubMsg()));
		PCollection<PubsubMessage> triggerMsg = write.apply(ParDo.of(new CreateMdstaffPubsubMsg()));
		triggerMsg.apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		
		pipeline.run();
	}
	
	public static class PerformProcessing extends PTransform<PCollection<PubsubMessage>, PCollection<TableRow>>{

		@Override
		public PCollection<TableRow> expand(PCollection<PubsubMessage> input) {
			
			PCollection<TableRow> writeObject = input.apply(ParDo.of(new InvokeApi()));
			
			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
			fields.add(new TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("jerseynumber").setType("INT64").setMode("NULLABLE"));
			TableSchema schema = new TableSchema().setFields(fields);
			
			
			writeObject.apply("Writing to BQ",
					BigQueryIO.writeTableRows().to("testdataset.new1www").withSchema(schema)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
							.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
			return writeObject;
		}
		
	}
	
	public static class InvokeApi extends DoFn<PubsubMessage, TableRow>{
		/*
		HttpClient client = null;
		@Setup
		public void Setup() {
			client = HttpClientBuilder.create().build(); 
		}
		*/
		@ProcessElement
		public void ProcessElement(ProcessContext ctx) {
			HttpClient client = HttpClientBuilder.create().build();    
		    HttpResponse response = null;
			try {
				response = client.execute(new HttpGet("https://www.stackoverflow.com"));
			} catch ( IOException e) {
				e.printStackTrace();
			}
		    int statusCode = response.getStatusLine().getStatusCode();
		   
		    TableRow outputTr = new TableRow();
		    outputTr.set("id", statusCode);
		    
		    ctx.output(outputTr);
		}
	}
	
	public static class CreateMdstaffPubsubMsg extends DoFn<TableRow, PubsubMessage>{
		@ProcessElement
		public void ProcessElement(ProcessContext ctx) throws UnsupportedEncodingException {
			TableRow obj = ctx.element().clone();
			HashMap<String, String> mapData = new HashMap<String, String>();
			
			byte[] data = String.valueOf(obj).getBytes("utf-8");
			PubsubMessage msg = new PubsubMessage(data, mapData);
			ctx.output(msg);
		}
	}
	
}
