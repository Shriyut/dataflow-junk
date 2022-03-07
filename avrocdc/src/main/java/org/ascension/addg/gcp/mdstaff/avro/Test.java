package org.ascension.addg.gcp.mdstaff.avro;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SampleIngestionOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		String schemaStr = "{\r\n"
				+ "    \"name\": \"MyClass\",\r\n"
				+ "    \"type\": \"record\",\r\n"
				+ "    \"namespace\": \"com.acme.avro\",\r\n"
				+ "    \"fields\": [\r\n"
				+ "        {\r\n"
				+ "            \"name\": \"id\",\r\n"
				+ "            \"type\": [ \"null\", \"string\" ]\r\n"
				+ "        },\r\n"
				+ "        {\r\n"
				+ "            \"name\": \"Name\",\r\n"
				+ "            \"type\": [ \"null\", \"string\" ]\r\n"
				+ "        },\r\n"
				+ "        {\r\n"
				+ "            \"name\": \"ts\",\r\n"
				+ "            \"type\": [ \"null\", \"string\" ]\r\n"
				+ "        }\r\n"
				+ "    ]\r\n"
				+ "}\r\n"
				+ "";
		
		Schema avroSchema = new Schema.Parser().parse(schemaStr);
		GenericRecord input = new Record(avroSchema);
		/*
		try {
			System.out.println(")))))))))))))))))");
			//PCollection<GenericRecord> newR = readAvroFile(pipeline, "gs://apdh-avro-test/aaempty.avro", avroSchema);
			PCollection<GenericRecord> avroRecord = pipeline.apply(AvroIO.readGenericRecords(avroSchema).from("gs://apdh-avro-test/aaempty.avro"));
		}catch(Exception e) {
			
		}*/
		
		//ResourceId x = FileSystems.matchNewResource("gs://apdh-avro-test/rtz.avro", false);
		
		try {
			Metadata b = FileSystems.matchSingleFileSpec("gs://apdh-avro-test/tyu.avro");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			PCollection<GenericRecord> check = pipeline.apply(Create.of(input).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
			check.apply("Write to avro test", AvroIO.writeGenericRecords(avroSchema)
					.to("gs://apdh-avro-test/xclaib3r")
					.withoutSharding()
					.withSuffix(".avro"));
		}
		
		/*
		PCollection<GenericRecord> check = pipeline.apply(Create.of(input).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
		check.apply("Write to avro test", AvroIO.writeGenericRecords(avroSchema)
				.to("gs://apdh-avro-test/aaempty")
				.withoutSharding()
				.withSuffix(".avro"));
				*/
		pipeline.run(options).waitUntilFinish();
	}
	
	public static PCollection<GenericRecord> readAvroFile(Pipeline pipeline, String gcsPath, Schema avroSchema) throws FileNotFoundException{
		return pipeline.apply(AvroIO.readGenericRecords(avroSchema).from(gcsPath));
	}

}
