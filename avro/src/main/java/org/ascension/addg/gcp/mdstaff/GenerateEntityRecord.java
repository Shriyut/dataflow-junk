package org.ascension.addg.gcp.mdstaff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

public class GenerateEntityRecord extends PTransform<PBegin, PCollection<KV<String, String>>>{

	
	private TableRow obj;
	private Pipeline p;

	public GenerateEntityRecord(TableRow obj, Pipeline p) {
		this.obj = obj;
		this.p = p;
	}

	@Override
	public PCollection<KV<String, String>> expand(PBegin input) {
		
		List<KV<String, String>> outputKV = new ArrayList<KV<String, String>>();
		
		obj.entrySet().stream().forEach((k)->{
			//List<KV<String, String>> tmpKV = new ArrayList<KV<String, String>>();
			List<KV<String, String>> tmpKV = Arrays.asList(KV.of(k.getKey(), String.valueOf(k.getValue())));
			outputKV.addAll(tmpKV);
		});
		
		PCollection<KV<String, String>> output = p.apply(Create.of(outputKV));
		
		return output;
	}
}
