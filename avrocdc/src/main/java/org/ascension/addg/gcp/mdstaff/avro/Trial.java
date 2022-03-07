package org.ascension.addg.gcp.mdstaff.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.mdstaff.avro.NewSample.ConvertGenericRecordToKV;
import org.ascension.addg.gcp.mdstaff.avro.NewSample.ConvertTableRowToGenericRecordFn;
import org.json.JSONObject;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import com.google.common.collect.Iterables;

import com.google.api.services.bigquery.model.TableRow;

public class Trial {

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
		
		//assuming api record
		GenericRecord input = new Record(avroSchema);
		GenericRecord finalInput = new Record(avroSchema);
		input.put("id", "10");
		input.put("Name", "Ozil");
		input.put("ts", "2980");
		GenericRecord input1 = new Record(avroSchema);
		input1.put("id", "21");
		input1.put("Name", "Pirlo");
		input1.put("ts", "8766");
		List<GenericRecord> arr = new ArrayList<>();
		arr.add(input);
		arr.add(input1);
		
		PCollection<GenericRecord> avroPC = pipeline.apply(Create.of(arr).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
		
		/*
		avroPC.apply("Write to avro test", AvroIO.writeGenericRecords(avroSchema)
				.to("gs://apdh-avro-test/assumingsourcerecords")
				.withoutSharding()
				.withSuffix(".avro"));
				
			*/	
		
		PCollection<GenericRecord> avroRecord = pipeline.apply(AvroIO.readGenericRecords(avroSchema).from("gs://apdh-avro-test/ascension.avro"));
		
		PCollection<String> sample = avroRecord.apply(ParDo.of(new DoFn<GenericRecord, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				c.output(String.valueOf(c.element()));
			}
		}));
		
		sample.apply(TextIO.write().to("gs://apdh-avro-test/checking").withoutSharding().withSuffix(".json"));
		/*
		PCollection<TableRow> players = pipeline
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM `jointest.players`").usingStandardSql());
		
		//assuming source records
		PCollection<GenericRecord> test = players.apply(ParDo.of(new ConvertTableRowToGenericRecordFn(avroSchema))).setCoder(AvroCoder.of(GenericRecord.class, avroSchema));
	
		
		PCollection<KV<String, GenericRecord>> leftKV = test.apply(MapElements.via(new ConvertGenericRecordToKV("id")));
		PCollection<KV<String, GenericRecord>> rightKV = avroPC.apply(MapElements.via(new ConvertGenericRecordToKV("id")));
		
		final TupleTag<GenericRecord> tag1 = new TupleTag<>();
		final TupleTag<GenericRecord> tag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> grouped = KeyedPCollectionTuple.of(tag1, leftKV)
				.and(tag2, rightKV)
				.apply(CoGroupByKey.create());
		
		PCollection<GenericRecord> ice = grouped.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, GenericRecord>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				KV<String, CoGbkResult> elem = c.element();
				Iterable<GenericRecord> leftItr = elem.getValue().getAll(tag1);
				Iterable<GenericRecord> rightItr = elem.getValue().getAll(tag2);
				
				if(Iterables.elementsEqual(leftItr, rightItr)) {
					
				}else {
					
				}
			}
		}));
		*/
		pipeline.run(options).waitUntilFinish();
		
	}
	
	public static class GetGenericRecordFn extends DoFn<KV<String, CoGbkResult>, GenericRecord>{
		
		private final String avroSchemaStr;
		private Schema avroSchema;
		private TupleTag rightTag;
		private TupleTag leftTag;
		
		public GetGenericRecordFn(Schema avroSchema, TupleTag leftTag, TupleTag rightTag) {
			avroSchemaStr = String.valueOf(avroSchema);
			this.leftTag = leftTag;
			this.rightTag = rightTag;
		}
		
		@Setup
		public void Setup() {
			avroSchema = new Schema.Parser().parse(avroSchemaStr);
		}
		
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			KV<String, CoGbkResult> elem = c.element();
			Iterable<GenericRecord> leftItr = elem.getValue().getAll(leftTag);
			Iterable<GenericRecord> rightItr = elem.getValue().getAll(rightTag);
			
			GenericRecord output = new Record(avroSchema);
			
			if(Iterables.elementsEqual(leftItr, rightItr)) {
	
			}else {
				
			}
		}
	}

}
