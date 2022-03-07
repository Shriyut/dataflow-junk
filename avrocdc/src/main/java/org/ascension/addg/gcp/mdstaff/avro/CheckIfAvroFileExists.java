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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.typesafe.config.Config;

public class CheckIfAvroFileExists extends DoFn<String, String> {

	private Config avroConfig;
	private String gcsPath;
	private Pipeline pipeline;

	public CheckIfAvroFileExists(Config avroConfig, String gcsPath, Pipeline pipeline) {
		
		this.avroConfig = avroConfig;
		this.gcsPath = gcsPath; //base gcs path
		this.pipeline = pipeline;
	}

	@ProcessElement
	public void ProcessElement(ProcessContext c) {
		String gcsPath = c.element(); //avro file base gcs path
		StringBuilder output = new StringBuilder();
		List<String> sources = avroConfig.getStringList("sources");
		sources.stream().forEach((k)->{
			Config tempCnf = avroConfig.getConfig(k);
			String schemaStr = tempCnf.getString("avroSchema");
			Schema avroSchema = new Schema.Parser().parse(schemaStr);
			String fileName = tempCnf.getString("avroFile");
			try {
				Metadata check = FileSystems.matchSingleFileSpec(gcsPath+fileName);
				output.append(".");
			} catch (IOException e) {
				GenericRecord input = new Record(avroSchema);
				PCollection<GenericRecord> avroContent = pipeline.apply(Create.of(input).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
				
				avroContent.apply("Write to avro file", AvroIO.writeGenericRecords(avroSchema)
						.to(gcsPath+fileName)
						.withoutSharding()
						.withSuffix(".avro"));
				output.append(".");
			}
		});
		
		c.output(String.valueOf(output));
	}
}
