package org.ascension.addg.gcp.abc;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;

public class EntryFn extends DoFn<Object, TableRow>{
	
	
		private String srcSystemName;
		private String entryValue;
		private String countColumn;
		private String trgtTableName;

		public EntryFn(String countColumn, String entryValue, String srcSystemName, String trgtTableName) {
			this.countColumn = countColumn;
			this.srcSystemName = srcSystemName;
			this.entryValue = entryValue;
			this.trgtTableName = trgtTableName;
		}

		
		
		int recordCount;
		
		@StartBundle
		public void StartBundle(StartBundleContext sbc) {
			recordCount = 0;
			
		}
		
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			
			++recordCount;
			
		}
		
		@FinishBundle
		public void FinishBundle(FinishBundleContext fbc) {
			
			
			TableRow output = AbcEntry.getEntry(entryValue, srcSystemName);
			PipelineOptions options = fbc.getPipelineOptions();
			DataflowWorkerHarnessOptions dwhoptions = options.as(DataflowWorkerHarnessOptions.class);
			
			String jobId = dwhoptions.getJobId();
			output.set("run_id", jobId);
			if(!countColumn.equalsIgnoreCase("NotApplicable")) {
				output.set(countColumn, recordCount);
			}
			
			if(!trgtTableName.equalsIgnoreCase("NotApplicable")) {
				output.set("target_tablename", trgtTableName);
			}

			fbc.output(output, Instant.now(), GlobalWindow.INSTANCE);
		}
}
