package FileValidation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import java.math.BigInteger;

public class TimeDiffCheck extends DoFn<String, String>{
	@ProcessElement
	public void ProcessElement(ProcessContext c) throws IOException {
		String fileName = c.element();
		
		try {
			//Reading source file
			ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(fileName, false));
			InputStream stream = Channels.newInputStream(channel);
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			StringBuilder dataBuilder = new StringBuilder();
			String line;
			
			List<BigInteger> datebig = new ArrayList<BigInteger>();
			while ((line = streamReader.readLine()) != null) {
				//Appending placeholders to process each line individually
				dataBuilder.append("$");
				dataBuilder.append(line);
				dataBuilder.append("\n");
				String testBuilder = dataBuilder.substring(dataBuilder.lastIndexOf("$") + 1,
						dataBuilder.lastIndexOf("\n"));
				String test = testBuilder.substring(testBuilder.lastIndexOf(",")+1).replaceAll("[^0-9]", "");
				
				if (test.isEmpty()) {
					//Do Nothing - for header value
				}else {
					
					BigInteger x = new BigInteger(test);
					datebig.add(x);
					
				}
			}
			// YYYYMMDDHHmmSS format - the latest date will be the maximum number
			BigInteger max = Collections.max(datebig);
			String sample = max.toString();
			BigInteger maxFileTime = new BigInteger(sample);
			
			//Extracts the last processed timestamp from statefile
			String stateT = extractState(Config.statePath);
			BigInteger stateFileTime = new BigInteger(stateT);
			
			int i = maxFileTime.compareTo(stateFileTime);
			
			if (i <=0) {
				c.output("false");
				System.out.println("false");
			}else {
				c.output("true");
				System.out.println("true");
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			c.output("false");
		}
	}
	
	public static String extractState(String path) {
		String stateFilePath = path;
		ReadableByteChannel channel2;
		String stateTime = "";
		int i =0;
		try {
			channel2 = FileSystems.open(FileSystems.matchNewResource(stateFilePath, false));
			InputStream stream2 = Channels.newInputStream(channel2);
			BufferedReader streamReader2 = new BufferedReader(new InputStreamReader(stream2, "UTF-8"));
			StringBuilder dataBuilder2 = new StringBuilder();
			String line2;
			
			while (( line2  = streamReader2.readLine()) != null) {
				dataBuilder2.append("$"); 
				dataBuilder2.append(line2);
				dataBuilder2.append("\n");
				String testBuilder = dataBuilder2.substring(dataBuilder2.lastIndexOf("$")+1, dataBuilder2.lastIndexOf("\n"));
				if (testBuilder.contains("medxcel")) {
					
					String t = testBuilder.substring(testBuilder.lastIndexOf(",")+1).replaceAll("[^0-9]", "");
					stateTime = t;
					
					
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(stateTime);
		return stateTime;
		
		
		
	}

}
