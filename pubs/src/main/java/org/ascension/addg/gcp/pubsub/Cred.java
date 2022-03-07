package org.ascension.addg.gcp.pubsub;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Cred {

	public static void main(String[] args) {
		
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		PCollection<TableRow> credential = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.credential_land LIMIT 100"));
		
		PCollection<TableRow> spl = credential.apply(ParDo.of(new ExtractLicenseFn("StateProfLicense")));
		PCollection<TableRow> dea = credential.apply(ParDo.of(new ExtractLicenseFn("DEA")));
		PCollection<TableRow> cds = credential.apply(ParDo.of(new ExtractLicenseFn("CDSCSR")));
		
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 

		fields.add(new TableFieldSchema().setName("CredentialID").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("ProviderID").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("Issued").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("Renewed").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("Expired").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("LicenseNumber").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("LicenseTypeID_Code").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("LicenseSubType_Code").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("LicensureBoardID_Code").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("State").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("StateProfessionID").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("Status").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("InUse").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("LastUpdated").setType("STRING").setMode("NULLABLE"));

		TableSchema schema = new TableSchema().setFields(fields);
		
		spl.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("asc-ahnat-apdh-sbx:apdh_test_dataset.aaspl")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		dea.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("asc-ahnat-apdh-sbx:apdh_test_dataset.aadea")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		cds.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("asc-ahnat-apdh-sbx:apdh_test_dataset.aacds")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		pipeline.run(options).waitUntilFinish();
	}
	
	public static class ExtractLicenseFn extends DoFn<TableRow, TableRow>{
		
		private String licenseType;
		public ExtractLicenseFn(String licenseType) {
			this.licenseType = licenseType;
		}
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			TableRow obj = c.element().clone();
			
			TableRow output = new TableRow();
			
			if(String.valueOf(obj.get("LicenseTypeID_Code")).equals(licenseType)) {
				// TODO: Automate via config file
				output.set("CredentialID", String.valueOf(obj.get("CredentialID")));
				output.set("ProviderID", String.valueOf(obj.get("ProviderID")));
				output.set("Issued", String.valueOf(obj.get("Issued")));
				output.set("Renewed", String.valueOf(obj.get("Renewed")));
				output.set("Expired", String.valueOf(obj.get("Expired")));
				output.set("LicenseNumber", String.valueOf(obj.get("LicenseNumber")));
				output.set("LicenseTypeID_Code", String.valueOf(obj.get("LicenseTypeID_Code")));
				output.set("LicenseSubType_Code", String.valueOf(obj.get("LicenseSubType_Code")));
				output.set("LicensureBoardID_Code", String.valueOf(obj.get("LicensureBoardID_Code")));
				output.set("State", String.valueOf(obj.get("State")));
				output.set("StateProfessionID", String.valueOf(obj.get("StateProfessionID")));
				output.set("Status", String.valueOf(obj.get("Status")));
				output.set("InUse", String.valueOf(obj.get("InUse")));
				output.set("LastUpdated", String.valueOf(obj.get("LastUpdated")));
				c.output(output);
			}
			//TableRow newTR = output;
			
			
		}
	}

}
