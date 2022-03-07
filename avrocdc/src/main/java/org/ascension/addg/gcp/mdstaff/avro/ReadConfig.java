package org.ascension.addg.gcp.mdstaff.avro;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.beam.sdk.io.FileSystems;

public class ReadConfig {
	public static String readConf(String input) throws IOException {
		
		String file;
		StringBuilder dataBuilder = new StringBuilder();
		try {
		ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(input, false));
		InputStream stream = Channels.newInputStream(channel);
		BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
		
		String line;
		
		while (( line  = streamReader.readLine()) != null) {
			dataBuilder.append(line);
			dataBuilder.append("\n");
			}
		
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		
		file = dataBuilder.toString();
		return file;
	}

}
