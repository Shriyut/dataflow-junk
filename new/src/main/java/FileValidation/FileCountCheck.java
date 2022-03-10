package FileValidation;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;

public class FileCountCheck extends DoFn<FileIO.ReadableFile, String>{
	int i =0;
	@ProcessElement
	public void ProcessElement(ProcessContext c) {
		System.out.println("Reading file"); // iterates based on number of files present in storage lcoation
		i++;
		if (i>1) {
			System.out.println("More files found");
			c.output("false");
		}else if (i==1) {
			System.out.println("One file");
			c.output("true");
		}
	}
}
