package samples;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;

public class testpipeline {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline p = Pipeline.create();
		
		PCollection<String> output = p.apply(TextIO.read().from("C:\\Users\\shrijha\\Downloads\\dept_data.txt"));
		output.apply(TextIO.write().to("C:\\Users\\shrijha\\Desktop\\sample.csv").withNumShards(1).withSuffix(".csv"));
		p.run();
	}

}
