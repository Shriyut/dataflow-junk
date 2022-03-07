package org.ascension.addg.gcp.mdstaff.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.ascension.addg.gcp.mdstaff.avro.NewSample.ConvertToTableRow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Setup {

	public static void main(String[] args) {
		SampleIngestionOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		String schemaStr = "{\r\n"
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
		
		String refSchema = "{\r\n"
				+ "	\"type\" : \"record\",\r\n"
				+ "	\"namespace\" : \"mdstaff\",\r\n"
				+ "	\"name\" : \"Site\",\r\n"
				+ "	\"fields\" : [\r\n"
				+ "		{ \"name\" : \"site_id\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"entity_id\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"site_type_code\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"site_type_description\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"name\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"tax_id_number\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"npi\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"medicaid_number\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"medicare_number\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"address1\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"address2\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"city\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"state_province\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"postal_code\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"county_id_code\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"county_id_description\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"pcmh_id_code\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"pcmh_id_description\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"start_date\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"end_date\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"in_use\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"last_updated\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"meta_api_object_id\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"meta_api_base_url\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"meta_src_system_name\", \"type\" :  [ \"null\", \"string\"] },\r\n"
				+ "		{ \"name\" : \"bq_load_timestamp\", \"type\" :  [ \"null\", \"string\"] }\r\n"
				+ "\r\n"
				+ "	]\r\n"
				+ "}\r\n"
				+ "";
		
		String config = "{\r\n"
				+ "	\"DemographicConfig\": {\r\n"
				+ "		\"avroFileLocation\": \"gs://apdh-avro-test/ascension.avro\",\r\n"
				+ "		\"primaryKey\": \"id\",\r\n"
				+ "	}\r\n"
				+ "}";
		
		Config avroConf = ConfigFactory.parseString(config).resolve();
		Config avroConfig = avroConf.getConfig("DemographicConfig");
		Schema avroSchema = new Schema.Parser().parse(schemaStr);
		PCollection<TableRow> players = pipeline
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM `jointest.playersscene1`").usingStandardSql());
		
		//PCollection<GenericRecord> source = pipeline.apply(AvroIO.readGenericRecords(avroSchema).from("gs://apdh-avro-test/assumingsourcerecords.avro"));
		PCollection<GenericRecord> source = pipeline.apply(AvroIO.readGenericRecords(avroSchema).from(avroConfig.getString("avroFileLocation")));
		
		PCollection<GenericRecord> ty = players.apply(new GenerateAvro(source, avroSchema, avroConfig));
		
		ty.apply("Write avro file to new location", AvroIO.writeGenericRecords(avroSchema)
				.to("gs://apdh-avro-test/ascension")
				.withoutSharding().withSuffix(".avro"));
		
		TableSchema bqSchema = new TableSchema().setFields(
				ImmutableList.of(
						new TableFieldSchema().setName("id").setType("STRING"),
						new TableFieldSchema().setName("Name").setType("STRING"),
						new TableFieldSchema().setName("ts").setType("STRING")
						));
		
		List<String> keys = new ArrayList<>();
		keys.add("id"); keys.add("Name"); keys.add("ts");
		PCollection<TableRow> convertedTR = ty.apply(MapElements.via(new ConvertToTableRow(keys)));
		
		convertedTR.apply(BigQueryIO.writeTableRows()
				.to("jointest.avrocdc")
				.withSchema(bqSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		pipeline.run(options).waitUntilFinish();
	}

}
