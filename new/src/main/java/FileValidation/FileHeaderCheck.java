package FileValidation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;

public class FileHeaderCheck extends DoFn<String, String>{
	@ProcessElement
	public void ProcessElement(ProcessContext c) throws IOException{
		
		String fileName = c.element();
		String headers = Config.headers;
		try {
			//Reading file
			ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(fileName, false));
			InputStream stream = Channels.newInputStream(channel);
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			StringBuilder dataBuilder = new StringBuilder();
			String line;
			
			while (( line  = streamReader.readLine()) != null) {
				// Extracts only the headers from the file
				dataBuilder.append(line);
				break;
			}
			String fileHeaders = dataBuilder.toString();
			
			if(fileHeaders.equals(headers)) {
				System.out.println("Right headers");
				c.output("true");
			}else {
				c.output("false");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			c.output("false");
		}
	}
}
