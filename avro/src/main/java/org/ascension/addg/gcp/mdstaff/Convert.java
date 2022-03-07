package org.ascension.addg.gcp.mdstaff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class Convert extends SimpleFunction<TableRow, List<KV<String, String>>>{
	
	public List<KV<String, String>> apply(TableRow input){
		
		List<KV<String, String>> inx = new ArrayList<>();
		input.entrySet().stream().forEach((k)->{
			//inx.add((KV<String, String>) Arrays.asList(KV.of(k.getKey(), k.getValue())));
			List<KV<String, String>> op = Arrays.asList(KV.of(k.getKey(), String.valueOf(k.getValue())));
			inx.addAll(op);
		});
		return inx;
		
	}
}
