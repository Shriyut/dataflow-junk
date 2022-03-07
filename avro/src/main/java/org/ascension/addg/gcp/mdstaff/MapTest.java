package org.ascension.addg.gcp.mdstaff;

import java.util.HashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class MapTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SampleIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		
		ImmutableList<TableRow> locationRow = ImmutableList.of(new TableRow().set("line1", "valline1").set("line2", "valline2"));
		PCollection<TableRow> locationPC = p.apply(Create.of(locationRow));
		ImmutableList<TableRow> siteRow = ImmutableList.of(new TableRow().set("line1", "input").set("site", "siteval1"));
		PCollection<TableRow> sitePC = p.apply(Create.of(siteRow));
		
		PCollection<HashMap<String, String>> locMap = locationPC.apply(MapElements.via(new MakeMap()));
		PCollection<HashMap<String, String>> siteMap = sitePC.apply(MapElements.via(new MakeMap()));
		
		PCollectionView<HashMap<String, String>> siteView = siteMap.apply(View.asSingleton());
		PCollection<HashMap<String, String>> unionMap = locMap.apply(ParDo.of(new DoFn<HashMap<String, String>, HashMap<String, String>>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				HashMap<String, String> element = c.element();
				HashMap<String, String> sideElement = c.sideInput(siteView);
				
				//TO DO -> Compare both maps and merge common keys and simply add other keys
			}
		}).withSideInputs(siteView));
		
	}
	
	public static HashMap<String, String> convertTableRowToMap(TableRow obj){
		
		HashMap<String, String> outputMap = new HashMap<String, String>();
		obj.entrySet().forEach((k)->{
			outputMap.put(String.valueOf(k.getKey()), String.valueOf(k.getValue()));
		});
		return outputMap;
		
	}
	
	public static class MakeMap extends SimpleFunction<TableRow, HashMap<String, String>>{
		
		public HashMap<String, String> apply(TableRow input){
			HashMap<String, String> outputMap = new HashMap<String, String>();
			
			input.entrySet().forEach((k)->{
				outputMap.put(String.valueOf(k.getKey()), String.valueOf(k.getValue()));
			});
			return outputMap;
			
		}
	}

}
