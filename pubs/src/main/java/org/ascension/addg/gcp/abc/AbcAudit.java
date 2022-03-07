package org.ascension.addg.gcp.abc;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class AbcAudit extends PTransform<PCollection<TableRow>, PDone> {
	
	private String tableName;
	public AbcAudit(String tableName) {
		this.tableName = tableName;
	}
	@Override
	public PDone expand(PCollection<TableRow> rows) {
		
		//Creation of schema for ABC audit table
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 
		fields.add(new TableFieldSchema().setName("run_id").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("job_start_time").setType("DATETIME").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("job_type").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("job_name").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("resource_type").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("job_status").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("job_fileName").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("target_tablename").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("src_system_name").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("processed_records").setType("INT64").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("valid_records").setType("INT64").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("invalid_records").setType("INT64").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("description").setType("STRING").setMode("NULLABLE"));
		//fields.add(new TableFieldSchema().setName("bq_load_timestamp").setType("DATETIME").setMode("NULLABLE"));
		
		TableSchema schema = new TableSchema().setFields(fields);
		
		//Write operation using beam BigQueryIO connector
		try {
		rows.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to(tableName)
				.withSchema(schema)
				
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));
		}catch(Exception e) {
			//e.printStackTrace();
		}
		
		return PDone.in(rows.getPipeline());
	}

}