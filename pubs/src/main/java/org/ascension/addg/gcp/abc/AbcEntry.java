package org.ascension.addg.gcp.abc;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import java.time.LocalDateTime;

import com.google.api.services.bigquery.model.TableRow;

public class AbcEntry {
	
	public static TableRow getEntry(String input, String srcSystemName) {
		
		TableRow output = new TableRow();
		
		switch(input) {
		
		case "BatchJobStart":
			output.set("description", "Job started");
			output.set("src_system_name", srcSystemName);
			output.set("job_start_time", getTime());
			output.set("job_status", "Started");
			output.set("job_type", "Batch");
			//output.set("bq_load_timestamp", getTime());
			break;
		case "DeliveryLayerJobStart":
			output.set("description", "Delivery Layer Job started");
			output.set("src_system_name", srcSystemName);
			output.set("job_status", "Started");
			output.set("job_type", "Streaming");
			//output.set("bq_load_timestamp", getTime());
			break;
		case "countEntry":
			output.set("description", "Number of records processed");
			output.set("src_system_name", srcSystemName);
			output.set("job_status", "Running");
			//output.set("bq_load_timestamp", getTime());
			break;
		case "PubsubCountEntry":
			output.set("description", "Number of records processed to pubsub");
			output.set("src_system_name", srcSystemName);
			output.set("job_status", "Running");
			//output.set("bq_load_timestamp", getTime());
			break;
		case "Finished":
			output.set("job_status", "Finished");
			output.set("description", "Job Execution completed");
			output.set("src_system_name", srcSystemName);
			//output.set("bq_load_timestamp", getTime());
			break;
		default:
				throw new RuntimeException("Valid ABC input not provided"+input);
		}
		
		return output;
	}
	
	public static void writeToAbcTable(PCollection<? extends Object> inputPC, String countColumn, 
			String entry, String abcTableName, String srcSystemName, String trgtTableName) {
		PCollection<Long> count = inputPC.apply(Count.globally());
		try {
			inputPC.apply(ParDo.of(new EntryFn(countColumn, entry, srcSystemName, trgtTableName))).apply(new AbcAudit(abcTableName));
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String getTime() {
		return LocalDateTime.now().toString();
	}
}