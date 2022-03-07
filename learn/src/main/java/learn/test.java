package learn;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Instant;
public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline p = Pipeline.create();
		PCollection<String> s = p.apply(Create.of("true"));
		PCollection<String> s2 = p.apply(Create.of("true"));
		PCollection<String> f = p.apply(Create.of("true"));
		
		PCollectionList<String> sample = PCollectionList.of(s).and(s2).and(f);
		PCollection<String> merged = sample.apply(Flatten.<String>pCollections());
		//merged.apply(TextIO.write().to("C:\\Users\\shrijha\\Desktop\\sampletest").withNumShards(1).withSuffix(".json"));
		
		PCollection<String> distinct = merged.apply(Distinct.<String>create());
		distinct.apply(TextIO.write().to("C:\\Users\\shrijha\\Desktop\\sampletest").withNumShards(1).withSuffix(".json"));
		distinct.apply(ParDo.of(new DoFn<String, String>(){
			int i =0;
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				if (c.element().equals("false")) {
					//System.out.println("lol");
					i++;
				}
			}
			
			@FinishBundle
			public void FinishBundle(FinishBundleContext c) {
				if (i == 1) {
					System.out.println("dfsf");
					c.output("true", Instant.now(), GlobalWindow.INSTANCE);
					//end pipeline here
				}else {
					System.out.println("iop[");
					c.output("true", Instant.now(), GlobalWindow.INSTANCE);
				}
			}
		}));
		//System.out.println("Done");
		
		
		
		
		p.run();
	}

}
