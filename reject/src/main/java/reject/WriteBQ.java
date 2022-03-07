package reject;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class WriteBQ extends PTransform<PCollection<TableRow>, PDone> {
	@Override
	public PDone expand(PCollection<TableRow> rows) {
		//if more attributes need to be added, schema has to be update here
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 
		fields.add(new TableFieldSchema().setName("reject_record").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("run_id").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("src_name").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("reject_reason").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("rejected_record_timestamp").setType("TIMESTAMP").setMode("NULLABLE"));
		
		String tableName = Config.tableName;
		TableSchema schema = new TableSchema().setFields(fields);
		try {
		rows.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to(tableName)
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		}catch(RuntimeException e) {
			e.printStackTrace();
		}
		return PDone.in(rows.getPipeline());
	}

}
