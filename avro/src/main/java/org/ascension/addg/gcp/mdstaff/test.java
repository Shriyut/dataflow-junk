package org.ascension.addg.gcp.mdstaff;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TableRow xcb = new TableRow();
		xcb.set("x", "k");
		xcb.set("x", "h");
		SampleIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		
		ImmutableList<TableRow> locationRow = ImmutableList.of(new TableRow().set("line1", "valline1").set("line2", "valline2"));
		PCollection<TableRow> locationPC = p.apply(Create.of(locationRow));
		ImmutableList<TableRow> siteRow = ImmutableList.of(new TableRow().set("line1", "input").set("site", "siteval1"));
		PCollection<TableRow> sitePC = p.apply(Create.of(siteRow));
		
		
		PCollection<TableRow> union = PCollectionList.of(locationPC).and(sitePC)
				.apply(Flatten.<TableRow>pCollections());
				
		
		PCollection<String> x = union.apply(ParDo.of(new DoFn<TableRow, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				c.output(obj.toString());
			}
		}));
		x.apply(TextIO.write().to("gs://symedical-apdh-test/asdfhh").withoutSharding().withSuffix(".json"));
		System.out.println(xcb.toString());
	}

}
