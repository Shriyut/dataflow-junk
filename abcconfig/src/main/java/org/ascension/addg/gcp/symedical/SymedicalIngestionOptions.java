package org.ascension.addg.gcp.symedical;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.commons.lang3.StringUtils;

public interface SymedicalIngestionOptions extends DataflowPipelineOptions{
	
	@Description("Config file location and other parameters")
	@Default.String(StringUtils.EMPTY)
	ValueProvider<String> getPipelineConfig();
	void setPipelineConfig(ValueProvider<String> value);
	
	@Description("Dataset which contains the ABC table")
	@Default.String(StringUtils.EMPTY)
	ValueProvider<String> getAbcDataset();
	void setAbcDataset(ValueProvider<String> value);
	
	@Description("ABC Table")
	@Default.String(StringUtils.EMPTY)
	ValueProvider<String> getAbcTable();
	void setAbcTable(ValueProvider<String> value);
}
