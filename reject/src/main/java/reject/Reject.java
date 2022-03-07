package reject;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class Reject extends DoFn<TableRow, TableRow>{
	StringBuilder s = new StringBuilder();
	String jobId;
	@ProcessElement
	public void ProcessElement(ProcessContext c) {
		//Concatenates each value with a delimiter for record record entry
		TableRow sample = c.element();
		String content = sample.values().toString();
		String values = content.substring(1, content.length()-1);
		
		String[] elements = values.split(", ");
		List<String> elem = Arrays.asList(elements);
		for(int i=0;i<elem.size();i++) {
			s.append(elem.get(i)+"|");
		}
		
		PipelineOptions options = c.getPipelineOptions();
		DataflowWorkerHarnessOptions dwhoptions = options.as(DataflowWorkerHarnessOptions.class);
		
		jobId = dwhoptions.getJobId();
	}
	
	@FinishBundle
	public void FinishBundle(FinishBundleContext c) {
		//System.out.println(s);
		String output = s.toString();
		// if other parameters are required then they can be added to table row object here
		TableRow outputRow = new TableRow().set("reject_record", output).set("run_id", jobId);
		c.output(outputRow, Instant.now(), GlobalWindow.INSTANCE);
	}

}
