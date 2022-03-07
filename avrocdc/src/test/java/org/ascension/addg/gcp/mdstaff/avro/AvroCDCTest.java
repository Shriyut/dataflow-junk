package org.ascension.addg.gcp.mdstaff.avro;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AvroCDCTest {

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
			+ "		\"avroSchema\": \"{\"name\": \"Demographic\",\"type\": \"Md-Staff\",\"namespace\": \"Md-staff\",\"fields\": [{ \"name\": \"provider_id\", \"type\": [ \"null\", \"string\" }]}\"\r\n"
			+ "		\"avroFile\": \"demographic.avro\",\r\n"
			+ "		\"primaryKey\": \"provider_id\",\r\n"
			+ "		\"keys\": [\r\n"
			+ "				\"provider_id\",\r\n"
			+ "		]\r\n"
			+ "	}\r\n"
			+ "\r\n"
			+ "}";
	
	Config avroConf = ConfigFactory.parseString(config).resolve();
	Pipeline p;
	@Test(expected = NullPointerException.class)
	public void createAvroTest() {
		AvroCDC.createAvro("test", testPipeline, avroSchemaStr);
	}
	
	@Test(expected = Exception.class)
	public void avroCDCTest() {
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("provider_id", "testVal1"));
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		
		testPipeline.run().waitUntilFinish();
	}
	}
}
