package org.ascension.addg.gcp.mdstaff;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.collections4.IterableUtils;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class NewJoin {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SampleIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		
		TableRow loc = new TableRow();
		loc.set("line1", "val1").set("line2", "val22").set("line3", "val3").set("line4", "val4");
		
		TableRow site = new TableRow();
		site.set("line1", "val2").set("site2", "sval2").set("site3", "sine3");
		
		PCollection<KV<String, String>> locPC = p.apply(new GenerateEntityRecord(loc, p));
		
		PCollection<KV<String, String>> sitePC = p.apply(new GenerateEntityRecord(site, p));
		
		final TupleTag<String> locTag = new TupleTag<>();
		final TupleTag<String> siteTag = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> results =
			    KeyedPCollectionTuple.of(locTag, locPC)
			        .and(siteTag, sitePC)
			        .apply(CoGroupByKey.create());
		
		PCollection<String> contactLines =
			    results.apply(
			        ParDo.of(
			        		
			            new DoFn<KV<String, CoGbkResult>, String>() {
			            	
			              @ProcessElement
			              public void processElement(ProcessContext c) {
			                KV<String, CoGbkResult> e = c.element();
			                String name = e.getKey();
			                Iterable<String> locIter = e.getValue().getAll(locTag);
			                Iterable<String> siteIter = e.getValue().getAll(siteTag);

			                StringBuffer s1 = new StringBuffer();
			                int l = IterableUtils.size(locIter);
			                int s = IterableUtils.size(siteIter);
			                System.out.println(l);
			                System.out.println("site"+s);
			                //s1.append(name);
			                //s1.append(locIter);
			                //s1.append(siteIter);
			                //op.set(e.getKey(), s1.toString());                    
			                //Iterator itr;
			                HashMap<String, String> hmap = new HashMap<String, String>();
			                hmap.put(name, locIter.toString()+siteIter.toString());
			                
			                
			                c.output(hmap.toString()); 
			                 
			            
			              }
			           }));
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("val1", "testVal1").set("val2", "testVal2"));
		PCollection<TableRow> inputPC = p.apply(Create.of(input));
		PCollection<List<KV<String, String>>> sam = inputPC.apply(MapElements.via(new Convert()));
		
		ImmutableList<TableRow> locationRow = ImmutableList.of(new TableRow().set("line1", "valline1").set("line2", "valline2"));
		PCollection<TableRow> locationPC = p.apply(Create.of(locationRow));
		//PCollection<KV<String, String>> sun = locationPC.apply(MapElements.via(new Convert()));
		
		final TupleTag<String> locTag1 = new TupleTag<>();
		final TupleTag<String> siteTag1 = new TupleTag<>();
		
		
		contactLines.apply(TextIO.write().to("gs://symedical-apdh-test/join").withoutSharding().withSuffix(".json"));
		p.run(options).waitUntilFinish();
	}

}
