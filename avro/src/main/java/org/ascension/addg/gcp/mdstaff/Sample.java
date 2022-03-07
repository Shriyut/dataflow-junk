package org.ascension.addg.gcp.mdstaff;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.ascension.addg.gcp.mdstaff.SampleIngestionOptions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Sample {

	public static void main(String[] args) throws IOException {
		
		SampleIngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline p = Pipeline.create(options);
		//String configUrl = options.getPipelineConfig().get();
		
		//String fileContent = ReadConfig.readConf(configUrl);
		//Config config = ConfigFactory.parseString(fileContent).resolve();
		
		//String schema = config.getString("avroschema");
		String schemauri = options.getAvroSchema().get();
		String schemaStr = "{"
				+ "\"type\": \"record\","
				+ "\"namespace\": \"Avrotest\","
				+ "\"name\": \"Avrotest\","
				+ "\"fields\": ["
				+ "{ \"name\": \"Name\", \"type\": \"string\"},"
				+ "{ \"name\": \"Value\", \"type\": \"int\"}"
				+ "]"
				+ "}";
		
		String input = "{\"Name\":\"Ozil\", \"Value\": 10}";
		
		Schema.Parser schemaParser = new Schema.Parser();
		Schema schema = schemaParser.parse(schemaStr);
		
		DecoderFactory decoderFactory = new DecoderFactory();
		Decoder decoder = decoderFactory.jsonDecoder(schema, input);
		DatumReader<GenericData.Record> reader =
	            new GenericDatumReader<>(schema);
		GenericRecord genericRecord = reader.read(null, decoder);
		
		PCollection<String> avroT = p.apply(Create.of(input));
		
		Schema schemanew = new Schema.Parser().parse(new File(schemauri));
		PCollection<GenericRecord> xx = p.apply(Create.of(genericRecord));
		
		xx.apply(AvroIO.writeGenericRecords(schemanew).to("gs://symedical-apdh-test/testavro").withSuffix(".avro").withoutSharding());
	
		p.run(options).waitUntilFinish();
	}

}
