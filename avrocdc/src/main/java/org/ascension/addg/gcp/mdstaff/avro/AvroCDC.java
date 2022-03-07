package org.ascension.addg.gcp.mdstaff.avro;

import java.io.IOException;

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

import com.google.api.services.bigquery.model.TableRow;
import com.typesafe.config.Config;

public class AvroCDC extends PTransform<PCollection<TableRow>, PCollection<TableRow>>{

	private String gcsBasePath;
	private String entityKey;
	private Config avroConfig;
	
	public AvroCDC(Config avroConfig, String entityKey, String gcsBasePath) {
		this.avroConfig = avroConfig;
		this.entityKey = entityKey;
		this.gcsBasePath = gcsBasePath;
	}
	@Override
	public PCollection<TableRow> expand(PCollection<TableRow> input) {
		
		Config entityConfig = avroConfig.getConfig(entityKey);
		Schema avroSchema;
		
		return null;
	}
	
	public static void CreateAvro(String gcsPath, Pipeline pipeline, String schemaStr) {
		Schema avroSchema = new Schema.Parser().parse(schemaStr);
		try {
			Metadata check = FileSystems.matchSingleFileSpec("gs://apdh-avro-test/tyu.avro");
		} catch (IOException e) {
			GenericRecord input = new Record(avroSchema);
			PCollection<GenericRecord> check = pipeline.apply(Create.of(input).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)));
			String gcsFileName = gcsPath.substring(gcsPath.lastIndexOf("/")+1, gcsPath.lastIndexOf("."));
			check.apply("Write to avro file", AvroIO.writeGenericRecords(avroSchema)
					.to(gcsFileName)
					.withoutSharding()
					.withSuffix(".avro"));
		}
	}

}
