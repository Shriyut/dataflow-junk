package streaming.test;

import static org.apache.beam.sdk.transforms.windowing.AfterProcessingTime.pastFirstElementInPane;

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
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class New {

	public static final int ALLOWED_LATENESS_SEC = 0;
	public static final int SESSION_WINDOW_GAP_DURATION = 10;

	static Logger LOG = LoggerFactory.getLogger(New.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);

		Pipeline pipeline = Pipeline.create(options);

		Trigger trigger = Repeatedly.forever(pastFirstElementInPane().orFinally(AfterWatermark.pastEndOfWindow()));

		Window<PubsubMessage> sessionWindow = Window
				.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardSeconds(SESSION_WINDOW_GAP_DURATION)))
				.triggering(trigger).withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_SEC))
				.accumulatingFiredPanes();
		PCollection<PubsubMessage> test = null;
		try {
			test = pipeline.apply(PubsubIO.readMessages()
					.fromSubscription("projects/sodium-chalice-334905/subscriptions/sampletopic-sub"));
		} catch (Exception e) {
			LOG.info("ERROR.......", e.getMessage());
		}
		// PCollection<PubsubMessage> test =
		// pipeline.apply(PubsubIO.readMessages().fromTopic("projects/sodium-chalice-334905/topics/sampletopic"));
		LOG.info("TEST.................");

		/*
		 * PCollection<PubsubMessage> result = test.apply("Applying sessionwindow",
		 * sessionWindow) .apply(new Test(pipeline, sessionWindow));
		 */
		
		PCollection<PubsubMessage> result = test.apply("Applying sessionwindow", sessionWindow)
				.apply(new TestWrite());

		/*
		 * Test t = new Test(pipeline, sessionWindow);
		 * t.expand(test).apply("Applying sessionwindow", sessionWindow);
		 */
		result.apply("Applying sessionwindow", sessionWindow)
				.apply(PubsubIO.writeMessages().to("projects/sodium-chalice-334905/topics/sampletopic"));
		pipeline.run();
	}

	public static class TestWrite extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

		@Override
		public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
			System.out.println("Hello Inside My Transformation Logic");
			return input;
		}

	}

	public static class Test extends PTransform<PCollection<PubsubMessage>, PCollection<PubsubMessage>> {

		private Pipeline pipeline;
		private Window session;

		public Test(Pipeline pipeline, Window session) {
			this.pipeline = pipeline;
			this.session = session;
		}

		@Override
		public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {

			PCollection<TableRow> rows = pipeline.apply(BigQueryIO.readTableRows()
					.fromQuery("SELECT * FROM `sodium-chalice-334905.testdataset.players`").usingStandardSql());
			rows.apply(session);

			List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
			fields.add(new TableFieldSchema().setName("id").setType("INT64").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("jerseynumber").setType("INT64").setMode("NULLABLE"));
			TableSchema schema = new TableSchema().setFields(fields);

			rows.apply("Writing to BQ",
					BigQueryIO.writeTableRows().to("testdataset.playersnew1").withSchema(schema)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
							.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
			// publish to pubsub code here
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
			msgPC.apply(session);
			// ImmutableList<TableRow> inputtr = ImmutableList.of(new
			// TableRow().set("run_id", "testVal1").set("run_error", "testVal2"));
			// PCollection<TableRow> inputPC = pipeline.apply(Create.<TableRow>of(inputtr));

			return msgPC;
		}

	}

}
