package org.ascension.addg.gcp.mdstaff.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

public class NewSample {

	
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
		
		Schema avroSchema = new Schema.Parser().parse(schemaStr);
		
		//assuming source records -> read from gcs
		PCollection<GenericRecord> source = pipeline.apply(AvroIO.readGenericRecords(avroSchema).from("gs://apdh-avro-test/assumingsourcerecords.avro"));
		
		PCollection<TableRow> players = pipeline
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM `jointest.playersscene1`").usingStandardSql());
		
		//assuming api records
		PCollection<GenericRecord> api = players.apply(ParDo.of(new ConvertTableRowToGenericRecordFn(avroSchema))).setCoder(AvroCoder.of(GenericRecord.class, avroSchema));
		
		
		//inner join between source and api records
		PCollection<KV<String, GenericRecord>> leftKV = source.apply(MapElements.via(new ConvertGenericRecordToKV("id")));
		PCollection<KV<String, GenericRecord>> rightKV = api.apply(MapElements.via(new ConvertGenericRecordToKV("id")));
		
		GenericRecord input = new GenericData.Record(avroSchema);
		//PCollection<KV<String, KV<GenericRecord, GenericRecord>>> result = Join.innerJoin(leftKV, rightKV);
		PCollection<KV<String, KV<GenericRecord, GenericRecord>>> result = Join.leftOuterJoin(leftKV, rightKV, input).setCoder(KvCoder.of(((KvCoder)leftKV.getCoder()).getKeyCoder(), KvCoder.of(((KvCoder)leftKV.getCoder()).getValueCoder(), ((KvCoder)rightKV.getCoder()).getValueCoder())));
		/*
		PCollection<String> rtz = result.apply(ParDo.of(new DoFn<KV<String, KV<GenericRecord, GenericRecord>>, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				System.out.println(c.toString());
				c.output(c.element().toString());
			}
		}));
		
		rtz.apply(TextIO.write().to("gs://apdh-avro-test/new").withoutSharding().withSuffix(".json"));
		*/
		
		PCollection<KV<String, String>> xz = api.apply(MapElements.via(new ReverseStringKV("id")));
		PCollection<KV<String, String>> yz = source.apply(MapElements.via(new ReverseStringKV("id")));
		//PCollection<KV<String, KV<String, String>>> result2 = Join.leftOuterJoin(yz, xz, "");
		PCollection<KV<String, KV<String, String>>> result2 = Join.fullOuterJoin(xz, yz, "empty", "empty");
		
		PCollection<GenericRecord> checki = result2.apply(ParDo.of(new PerformAvroCdcFn(avroSchema))).setCoder(AvroCoder.of(GenericRecord.class, avroSchema));
		
		PCollection<String> rtz = result2.apply(ParDo.of(new DoFn<KV<String, KV<String, String>>, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				System.out.println(c.toString());
				c.output(c.element().toString());
			}
		}));
		
		
		PCollection<String> fly = checki.apply(ParDo.of(new DoFn<GenericRecord, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				c.output(String.valueOf(c.element()));
			}
		}));
		fly.apply(TextIO.write().to("gs://apdh-avro-test/newfly").withoutSharding().withSuffix(".json"));
		
		checki.apply("Write avro file to new location", AvroIO.writeGenericRecords(avroSchema)
				.to("gs://apdh-avro-test/ascension")
				.withoutSharding().withSuffix(".avro"));
		rtz.apply(TextIO.write().to("gs://apdh-avro-test/new").withoutSharding().withSuffix(".json"));
		
		List<String> keys = new ArrayList<>();
		keys.add("id"); keys.add("Name"); keys.add("ts");
		PCollection<TableRow> convertedTR = checki.apply(MapElements.via(new ConvertToTableRow(keys)));
		
		TableSchema bqSchema = new TableSchema().setFields(
				ImmutableList.of(
						new TableFieldSchema().setName("id").setType("STRING"),
						new TableFieldSchema().setName("Name").setType("STRING"),
						new TableFieldSchema().setName("ts").setType("STRING")
						));
		
		convertedTR.apply(BigQueryIO.writeTableRows()
				.to("jointest.avrocdc")
				.withSchema(bqSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		/*
		test.apply("Write to avro test", AvroIO.writeGenericRecords(avroSchema)
				.to("gs://apdh-avro-test/loltest")
				.withoutSharding()
				.withSuffix(".avro"));
		
		
		avroPC.apply(BigQueryIO.<GenericRecord>write()
				.to("jointest.testavro")
				.withJsonSchema(schemaStr) //use beam schema, this will only work with a formatting function
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
		        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
		        .optimizedWrites());
		*/
		pipeline.run(options).waitUntilFinish();
	}
	
	public static class ConvertTableRowToGenericRecordFn extends DoFn<TableRow, GenericRecord>{
		
		private final String avroSchemaStr;
		private Schema avroSchema;

		public ConvertTableRowToGenericRecordFn(Schema avroSchema) {
			avroSchemaStr = String.valueOf(avroSchema);
		}
		
		@Setup
		public void Setup() {
			avroSchema = new Schema.Parser().parse(avroSchemaStr);
		}
		
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			TableRow obj = c.element().clone();
			
			GenericRecord output = new GenericData.Record(avroSchema);
			
			obj.entrySet().forEach((k)->{
				output.put(k.getKey(), k.getValue());
			});
			
			c.output(output);
		}
	}
	
	public static class ConvertGenericRecordToKV extends SimpleFunction<GenericRecord, KV<String, GenericRecord>>{
		
		private String key;

		public ConvertGenericRecordToKV(String key) {
			this.key = key;
		}
		
		public KV<String, GenericRecord> apply(GenericRecord input){
			
			return KV.of(String.valueOf(input.get(key)), input);
			
		}
	}
	
	
	public static class ReverseStringKV extends SimpleFunction<GenericRecord, KV<String, String>>{
		
		//primary key
		private String key;

		public ReverseStringKV(String key) {
			this.key = key;
		}
		
		public KV<String, String> apply(GenericRecord input){
			
			return KV.of(String.valueOf(input.get(key)), String.valueOf(input));
			
		}
	}
	
	public static class PerformAvroCdcFn extends DoFn<KV<String, KV<String, String>>, GenericRecord>{
		
		private final String avroSchemaStr;
		private Schema avroSchema;
		
		public PerformAvroCdcFn(Schema avroSchema) {
			avroSchemaStr = String.valueOf(avroSchema);
		}
		
		@Setup
		public void Setup() {
			avroSchema = new Schema.Parser().parse(avroSchemaStr);
		}
		
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			KV<String, String> elem = c.element().getValue();
			String updatedValue = elem.getKey();
			String existingValue = elem.getValue();
			JsonAvroConverter avroConverter = new JsonAvroConverter();
			GenericRecord output;
			
			if(updatedValue.equals("empty")) {
				output = avroConverter.convertToGenericDataRecord(existingValue.getBytes(), avroSchema);
				c.output(output);
			}else{
				output = avroConverter.convertToGenericDataRecord(updatedValue.getBytes(), avroSchema);
				c.output(output);
			}
			
		}
	}
	
	public static class ConvertToTableRow extends SimpleFunction<GenericRecord, TableRow>{
		
		
		private List<String> keys;

		public ConvertToTableRow(List<String> keys) {
			this.keys = keys;
		}
		
		public TableRow apply(GenericRecord input) {
			TableRow output = new TableRow();
			keys.stream().forEach((k)->{
				output.set(k, String.valueOf(input.get(k)));
			});
			return output;
		}
	}
}
