package org.ascension.addg.gcp.mdstaff;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Join {

	public static void main(String[] args) {
		
		SampleIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		
		//location -> address + site
		String location = "\"val1\": ";
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("val1", "testVal1").set("val2", "testVal2"));
		PCollection<TableRow> inputPC = p.apply(Create.of(input));
		
		ImmutableList<TableRow> locationRow = ImmutableList.of(new TableRow().set("line1", "valline1").set("line2", "valline2"));
		PCollection<TableRow> locationPC = p.apply(Create.of(locationRow));
		ImmutableList<TableRow> siteRow = ImmutableList.of(new TableRow().set("line1", "input").set("site", "siteval1"));
		PCollection<TableRow> sitePC = p.apply(Create.of(siteRow));
		
		//MapElements<TableRow, KV<String, String>> locKV = MapElements.into(kvs(strings(), strings()))
			//	.via(obj -> KV.of(obj.getKey(), obj.getValue()));
		PCollectionView<TableRow> siteInput = sitePC.apply(View.asSingleton());
		locationPC.apply(ParDo.of(new DoFn<TableRow, KV<String, CoGbkResult>>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				//List<KV<String, String>> out = generateList(c.element());
				PCollection<KV<String, String>> hj = p.apply(new GenerateEntityRecord(c.element(), p));
				TableRow sideInput = c.sideInput(siteInput);
				PCollection<KV<String, String>> tt = p.apply(new GenerateEntityRecord(sideInput, p));
				final TupleTag<String> hjTag = new TupleTag<>();
				final TupleTag<String> ttTag = new TupleTag<>();
				
				PCollection<KV<String, CoGbkResult>> results =
					    KeyedPCollectionTuple.of(hjTag, hj)
					        .and(ttTag, tt)
					        .apply(CoGroupByKey.create());
				
				
				//c.output(results);
			}
		}).withSideInputs(siteInput));
		
		PCollection<KV<String, String>> xx = locationPC.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
				.via(tableRow -> KV.of((String) tableRow.get("val1"), (String) tableRow.get("val2"))));
		
		
		
		//PCollection<KV<String, String>> locationKV = generateList(locationRow, p);
		
		p.run(options).waitUntilFinish();
	}
	
	public static List<KV<String, String>> generateList(TableRow obj){
		
		List<KV<String, String>> outputKV = new ArrayList<KV<String, String>>();
		
		obj.entrySet().stream().forEach((k)->{
			//List<KV<String, String>> tmpKV = new ArrayList<KV<String, String>>();
			List<KV<String, String>> tmpKV = Arrays.asList(KV.of(k.getKey(), String.valueOf(k.getValue())));
			outputKV.addAll(tmpKV);
		});
		
		
		return outputKV;
		
	}

}
