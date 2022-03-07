package streaming.test;

import java.util.HashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;


public class Local {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline p = Pipeline.create();
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("run_id", "testVal1").set("run_error", "testVal2"));
		PCollection<TableRow> inputPC = p.apply(Create.<TableRow>of(input));
		
		ImmutableList<TableRow> input1 = ImmutableList.of(new TableRow().set("run_id", "testVal11").set("run_error1", "testVal21"));
		PCollection<TableRow> inputPC1 = p.apply(Create.<TableRow>of(input1));
		
		HashMap<String, PCollection<TableRow>> m = new HashMap<String, PCollection<TableRow>>();
		m.put("one", inputPC);
		m.put("two", inputPC1);
		
		m.get("two").apply(ParDo.of(new DoFn<TableRow, Void>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		p.run();
	}
	
	

}
