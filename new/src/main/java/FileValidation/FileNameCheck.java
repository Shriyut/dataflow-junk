package FileValidation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class FileNameCheck extends DoFn<String, String> {
	@ProcessElement
	public void ProcessElement(ProcessContext c) throws IOException{
		String fileName = c.element();
		
		try {
		//Reading File
		ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(fileName, false));
		if ( channel!=null) {
			System.out.println("File present");
			c.output("true");
		}
		}catch(FileNotFoundException e) {
			System.out.println("False");
			c.output("false");
		}
		
		
	}

}
