package org.ascension.addg.gcp.mdstaff.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.typesafe.config.Config;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class GenerateAvro extends PTransform<PCollection<TableRow>, PCollection<GenericRecord>>{

	private PCollection<GenericRecord> gcsContent;
	private Schema avroSchema;
	private Config avroConfig;
	public GenerateAvro(PCollection<GenericRecord> gcsContent, Schema avroSchema, Config avroConfig) {
		this.gcsContent = gcsContent;
		this.avroSchema = avroSchema;
		this.avroConfig = avroConfig;
	}
	@Override
	public PCollection<GenericRecord> expand(PCollection<TableRow> input) {
		PCollection<KV<String, String>> rightKV = gcsContent.apply("Convert to KV", MapElements.via(new ConvertGenericRecordToKVString(avroConfig.getString("primaryKey"))));
		PCollection<GenericRecord> leftKVInput = input.apply("Create GenericRecord", ParDo.of(new ConvertTableRowToGenericRecordFn(avroSchema))).setCoder(AvroCoder.of(GenericRecord.class, avroSchema));
		
		PCollection<KV<String, String>> leftKV = leftKVInput.apply("Convert to KV right", MapElements.via(new ConvertGenericRecordToKVString(avroConfig.getString("primaryKey"))));
		
		PCollection<KV<String, KV<String, String>>> joinedResult = Join.fullOuterJoin(leftKV, rightKV, "empty", "empty");
		
		PCollection<GenericRecord> output = joinedResult.apply("Final result", ParDo.of(new PerformAvroCdcFn(avroSchema))).setCoder(AvroCoder.of(GenericRecord.class, avroSchema));
		
		return output;
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
	
	public static class ConvertGenericRecordToKVString extends SimpleFunction<GenericRecord, KV<String, String>>{
		
		//primary key
		private String key;

		public ConvertGenericRecordToKVString(String key) {
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
	
	public static class ConvertGenericRecordToTableRow extends SimpleFunction<GenericRecord, TableRow>{
		
		
		private List<String> keys;

		public ConvertGenericRecordToTableRow(List<String> keys) {
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
