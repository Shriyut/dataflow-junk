package reject;


import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import reject.Reject;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;


public class Sample {
	public static void main(String [] args) {
		//DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		//options.setProject("spry-abacus-325317");
		//options.setTempLocation("gs://dataflow_test1234/temp");
		//options.setStagingLocation("gs://dataflow_test1234/output");
		//options.setRunner(DataflowRunner.class);
		//options.setRegion("us-central1");
		//Pipeline pipeline= Pipeline.create(options);
		
		DataflowPipelineOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		//creating a demo value for testing purpose
		TableRow row = new TableRow().set("run_id", "sample").set("run_error", "testval");
		PCollection<TableRow> rows = pipeline.apply(Create.of(ImmutableList.of(row)));
		PCollection<TableRow> rejectRecord = rows.apply(ParDo.of(new Reject()));
		rejectRecord.apply(new WriteBQ());

		pipeline.run(options).waitUntilFinish();
		
	}

}