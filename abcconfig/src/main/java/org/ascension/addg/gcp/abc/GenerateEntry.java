package org.ascension.addg.gcp.abc;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;


public class GenerateEntry extends DoFn<TableRow, TableRow>{
	String jobId;
	@ProcessElement
	public void ProcessElement(ProcessContext c) throws Exception {
		
		//ParDo operation to add run id of the dataflow job to audit entry for ABC audit table
		PipelineOptions options = c.getPipelineOptions();
		DataflowWorkerHarnessOptions dwhoptions = options.as(DataflowWorkerHarnessOptions.class);
		
		jobId = dwhoptions.getJobId();
		c.output(c.element().set("run_id", jobId));
		
	}
}