package org.ascension.addg.gcp.symedical;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.ascension.addg.gcp.abc.AbcAudit;
import org.ascension.addg.gcp.abc.GenerateEntry;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;


public class Symedical {

	public static void main(String[] args) {
		SymedicalIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SymedicalIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		
		String abcDataset = options.getAbcDataset().get();
		String abcTable = options.getAbcTable().get();
		
		String tablePath = abcDataset+"."+abcTable;
		
		TableRow sample = new TableRow().set("description", "sample entry");
		
		PCollection<TableRow> entry = p.apply(Create.of(ImmutableList.of(sample)));
		PCollection<TableRow> updatedEntry = entry.apply(ParDo.of(new GenerateEntry()));
		updatedEntry.apply(new AbcAudit(tablePath));
		
		p.run(options).waitUntilFinish();

	}

}
