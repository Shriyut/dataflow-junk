package samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class Check {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline p = Pipeline.create();
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("run_id", "testVal1").set("run_error", "testVal2"));
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		p.run();
	}

}
