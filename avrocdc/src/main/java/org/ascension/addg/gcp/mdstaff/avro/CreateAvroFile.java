package org.ascension.addg.gcp.mdstaff.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import com.typesafe.config.Config;

public class CreateAvroFile extends PTransform<PCollection<String>, PCollection<String>>{

	
	private Pipeline pipeline;
	private Config avroConfig;
	private String gcsPath;
	public CreateAvroFile(Pipeline pipeline, Config avroConfig, String gcsPath) {
		this.pipeline = pipeline;
		this.avroConfig = avroConfig;
		this.gcsPath = gcsPath;
	}
	@Override
	public PCollection<String> expand(PCollection<String> input) {
		List<String> sources = avroConfig.getStringList("sources");
		
		StringBuilder output = new StringBuilder();
		String returnValue = null;
		
		for(int i=0;i<sources.size();i++) {
			String tableName = sources.get(i);
			Config tempConf = avroConfig.getConfig(tableName);
			String schemaStr = tempConf.getString("avroSchema");
			Schema avroSchema = new Schema.Parser().parse(schemaStr);
			String avroFileName = tempConf.getString("avroFile");
			String fileName = avroFileName.substring(0, avroFileName.lastIndexOf("."));
			try {
				Metadata check = FileSystems.matchSingleFileSpec(gcsPath+avroFileName);
				output.append(".");
			} catch (IOException e) {
				GenericRecord avroInput = new Record(avroSchema);
				PCollection<GenericRecord> avroContent = pipeline.apply(Create.of(avroInput).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
				
				avroContent.apply("Write to avro file", AvroIO.writeGenericRecords(avroSchema)
						.to(gcsPath+fileName)
						.withoutSharding()
						.withSuffix(".avro"));
				output.append(".");
			}
			
			returnValue = String.valueOf(output);
		}
		
		
		return pipeline.apply(Create.of(returnValue));
	}

}
