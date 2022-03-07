package org.ascension.addg.gcp.mdstaff.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class GenerateAvroTest {

	@Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
	
	String avroSchemaStr = "{\r\n"
			+ "    \"name\": \"MyClass\",\r\n"
			+ "    \"type\": \"record\",\r\n"
			+ "    \"namespace\": \"com.acme.avro\",\r\n"
			+ "    \"fields\": [\r\n"
			+ "        {\r\n"
			+ "            \"name\": \"id\",\r\n"
			+ "            \"type\": \"string\"\r\n"
			+ "        },\r\n"
			+ "        {\r\n"
			+ "            \"name\": \"Name\",\r\n"
			+ "            \"type\": \"string\"\r\n"
			+ "        },\r\n"
			+ "        {\r\n"
			+ "            \"name\": \"ts\",\r\n"
			+ "            \"type\": \"string\"\r\n"
			+ "        }\r\n"
			+ "    ]\r\n"
			+ "}\r\n"
			+ "";
	
	Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
	
	String config = "{\r\n"
			+ "	\"DemographicConfig\": {\r\n"
			+ "		\"avroFileLocation\": \"gs://apdh-avro-test/ascension.avro\",\r\n"
			+ "		\"primaryKey\": \"id\",\r\n"
			+ "	}\r\n"
			+ "}";
	
	Config avroConf = ConfigFactory.parseString(config).resolve();
	Config avroConfig = avroConf.getConfig("DemographicConfig");
	
	@Test
	public void generateAvroUpdateTest() {
		//simulates updated record
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("id", "10").set("Name", "Ozil").set("ts", "11"));
		PCollection<TableRow> inputPC = testPipeline.apply("Sample create", Create.<TableRow>of(input));
		
		//simulates source file
		GenericRecord avroInput = new GenericData.Record(avroSchema);
		avroInput.put("id", "10");
		avroInput.put("Name", "Ozil");
		avroInput.put("ts", "1111");
		
		PCollection<GenericRecord> avroFile = testPipeline.apply("Create avro", Create.of(avroInput).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
		PCollection<GenericRecord> outputPC = inputPC.apply(new GenerateAvro(avroFile, avroSchema, avroConfig));
		
		GenericRecord avroOutput1 = new GenericData.Record(avroSchema);
		avroOutput1.put("id", "10");
		avroOutput1.put("Name", "Ozil");
		avroOutput1.put("ts", "11");
		
		
		PAssert.that(outputPC).containsInAnyOrder(avroOutput1);
		testPipeline.run().waitUntilFinish();
	}
	
	@Test(expected = AssertionError.class)
	public void generateAvroTest() {
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("id", "10").set("Name", "Ozil").set("ts", "19"));
		PCollection<TableRow> inputPC = testPipeline.apply("Sample create", Create.<TableRow>of(input));
		
		GenericRecord avroInput = new GenericData.Record(avroSchema);
		avroInput.put("id", "21");
		avroInput.put("Name", "Pirlo");
		avroInput.put("ts", "11");
		
		PCollection<GenericRecord> avroFile = testPipeline.apply("Create avro", Create.of(avroInput).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
		PCollection<GenericRecord> outputPC = inputPC.apply(new GenerateAvro(avroFile, avroSchema, avroConfig));
		GenericRecord avroOutput = new GenericData.Record(avroSchema);
		avroOutput.put("id", "21");
		avroOutput.put("Name", "Pirlo");
		avroOutput.put("ts", "5467");
		
		GenericRecord avroOutput1 = new GenericData.Record(avroSchema);
		avroOutput1.put("id", "10");
		avroOutput1.put("Name", "Ozil");
		avroOutput1.put("ts", "19");
		
		List<GenericRecord> check = new ArrayList<>();
		check.add(avroOutput);
		check.add(avroOutput1);
		
		List<String> keys = new ArrayList<>();
		keys.add("id"); keys.add("Name"); keys.add("ts");
		
		PCollection<TableRow> outputTr = outputPC.apply(MapElements.via(new GenerateAvro.ConvertGenericRecordToTableRow(keys)));
		TableRow checkTr = new TableRow();
		checkTr.set("id", "21");
		checkTr.set("Name", "Pirlo");
		checkTr.set("ts", "11");
		PAssert.that(outputTr).containsInAnyOrder(checkTr);
		testPipeline.run().waitUntilFinish();
	}
}
