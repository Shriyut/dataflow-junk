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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class Check {

	public static final int ALLOWED_LATENESS_SEC = 0;
	public static final int SESSION_WINDOW_GAP_DURATION = 10;

	public static void main(String[] args) {

		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);
		options.setStreaming(true);
		Pipeline pipeline = Pipeline.create(options);

		// TODO: if it's actually cyclical, you want SessionWindow (confirm) w/ a decent TS
		// TODO: if it is or if it isn't, querying BQ for each message is inefficient; think through design & performance
		// TODO: look into custom I/O for API
		pipeline.apply(PubsubIO.readMessages()
				.fromSubscription("projects/sodium-chalice-334905/subscriptions/sampletopic-sub"))
				.apply("Extract Query", ParDo.of(new extractQueryDoFn()))
				.apply(PubsubIO.writeStrings().to("projects/sodium-chalice-334905/topics/sampletopic"));

		pipeline.run();
	}

	public static class extractQueryDoFn extends DoFn<PubsubMessage, String> {

		@ProcessElement
		public void ProcessElement(ProcessContext ctx) {
			PubsubMessage msg = ctx.element();
			byte[] val = msg.getPayload();
			Map<String, String> msgAttributes = msg.getAttributeMap();
			System.out.println(String.valueOf(msgAttributes));
			try {
				String q = msgAttributes.get("query");
			} catch (NullPointerException e) {
				System.out.println("uiop");
			}
			String query = new String(val, StandardCharsets.UTF_8);

			System.out.println("extractQueryDoFn: Extracted query " + query);
			// TODO: error handling etc
			ctx.output(query);
		}
	}

	public static class queryDoFn extends DoFn<String, String> {
		// On each worker, this has to be serialized
		// Most of the google libraries cannot be serialized
		private transient BigQuery bq;

		public queryDoFn() {
			init();
		}

		@Setup
		private void init() {
			// Before processing any element, make sure the objects that can't be serialized
			// are not null
			if (this.bq == null) {
				this.bq = BigQueryOptions.getDefaultInstance().getService();
				// etc.
			}
		}

		@ProcessElement
		public void ProcessElement(ProcessContext ctx) throws JobException, InterruptedException {
			String query = ctx.element();

			QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder(query).build();
			TableResult res = bq.query(cfg);
			System.out.println("queryDoFn: Querying BQ with " + query);
			// TODO: parse results
			for (Object o : res.iterateAll()) {
				// TODO: parse, get fields, etc.
				ctx.output(res.toString());
			}
		}
	}

	/*
	 * private void test() { PCollection<TableRow> rows = null; try { rows =
	 * pipeline.apply(BigQueryIO.readTableRows()
	 * .fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").
	 * usingStandardSql()); } catch (NullPointerException e) {
	 * System.out.println("caught"); e.printStackTrace(); }
	 * 
	 * List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
	 * fields.add(new
	 * TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
	 * fields.add(new
	 * TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
	 * fields.add(new
	 * TableFieldSchema().setName("jerseynumber").setType("INT64").setMode(
	 * "NULLABLE")); TableSchema schema = new TableSchema().setFields(fields);
	 * 
	 * rows.apply("Writing to BQ",
	 * BigQueryIO.writeTableRows().to("testdataset.playersneww").withSchema(schema)
	 * .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	 * .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
	 * .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
	 * 
	 * HashMap<String, String> mapData = new HashMap<String, String>(); JSONObject
	 * jsonData = new JSONObject(); //
	 * jsonData.put("SELECT * FROM `sodium-chalice-334905.testdataset.players`");
	 * mapData.put("query",
	 * "SELECT * FROM `sodium-chalice-334905.testdataset.players`"); String qu =
	 * "SELECT * FROM `sodium-chalice-334905.testdataset.players`"; byte[] data =
	 * null; try { data = qu.toString().getBytes("utf-8"); } catch
	 * (UnsupportedEncodingException e) {
	 * 
	 * } PubsubMessage msgnew = new PubsubMessage(data, mapData); //
	 * PCollection<PubsubMessage> msgPC = pipeline.apply(Create.of(msgnew));
	 * ctx.output(msgnew); }
	 */
	// }

}
