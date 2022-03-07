package org.ascension.addg.gcp.mdstaff.avro;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.commons.lang3.StringUtils;

public interface SampleIngestionOptions extends DataflowPipelineOptions{
	
	@Description("Config file location and other parameters")
	@Default.String(StringUtils.EMPTY)
	ValueProvider<String> getPipelineConfig();
	void setPipelineConfig(ValueProvider<String> value);
	
	@Description("Statefile gcs location")
	@Default.String(StringUtils.EMPTY)
	ValueProvider<String> getAvrofileGcsPath();
	void setAvrofileGcsPath(ValueProvider<String> value);
}
