package org.ascension.addg.gcp.pubsub;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.Flatten;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LocationTest {

	public static void main(String[] args) {
		
		
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		PCollection<TableRow> address = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.address_land LIMIT 1000"));
		PCollection<TableRow> site = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.site_land LIMIT 1000"));
	
		PCollection<TableRow> addressTransformed = address.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				TableRow op = new TableRow();
				
				
				op.set("Country", String.valueOf(obj.get("CountryID_Code")));
				
				op.set("City", String.valueOf(obj.get("City")));
				op.set("AddressLine1", String.valueOf(obj.get("Address")));
				op.set("AddressLine2", String.valueOf(obj.get("Address2")));
				op.set("StateProvince", String.valueOf(obj.get("State")));
				op.set("SubAdministrativeArea", String.valueOf(obj.get("CountyID_Code")));
				op.set("PublishToExternalDirectoryFlag", String.valueOf(obj.get("PubAddrExtDir")));
				op.set("PublishToExternalDirectoryPrimaryFlag", String.valueOf(obj.get("PubAddrExtPrim")));
				op.set("AddressInUse", String.valueOf(obj.get("InUse")));
				
				TableRow providerIdentifier = new TableRow();
				providerIdentifier.set("SystemIdentifierType", "ProviderID");
				providerIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("ProviderID")));
				
				TableRow addressIdentifier = new TableRow();
				addressIdentifier.set("SystemIdentifierType", "AddressID");
				addressIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("AddressID")));
				
				//TableRow siteIdentifier = new TableRow();
				//siteIdentifier.set("SystemIdentifierType", "SiteID");
				//siteIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("SiteID")));
				
				TableRow[] arr = new TableRow[2];
				//arr[0] = siteIdentifier;
				arr[0] = addressIdentifier;
				arr[1] = providerIdentifier;
				
				op.set("SystemIdentifier", arr);
				
				TableRow crosswalk = new TableRow();
				crosswalk.set("type", "configuration/sources/MDStaff");
				crosswalk.set("value", String.valueOf(obj.get("AddressID")));
				
				TableRow zip = new TableRow();
				zip.set("PostalCode", String.valueOf(obj.get("Zip")));
				zip.set("Zip5", "");
				zip.set("Zip4", "");
				
				op.set("Zip", zip);
				
				op.set("crosswalks", crosswalk);
				
				c.output(op);
			}
		}));
		
		PCollection<TableRow> siteTransformed = site.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj1 = c.element().clone();
				TableRow op1 = new TableRow();
				
				op1.set("AddressLine1", String.valueOf(obj1.get("Address1")));
				op1.set("AddressLine2", String.valueOf(obj1.get("Address2")));
				op1.set("City", String.valueOf(obj1.get("City")));
				op1.set("SubAdministrativeArea", String.valueOf(obj1.get("CountyID_Code")));
				op1.set("StateProvince", String.valueOf(obj1.get("StateProvince")));
				op1.set("Country", "");
				op1.set("PublishToExternalDirectoryFlag", "");
				op1.set("PublishToExternalDirectoryPrimaryFlag", "");
				op1.set("AddressInUse", String.valueOf(obj1.get("InUse")));
				
				TableRow siteIdentifier = new TableRow();
				siteIdentifier.set("SystemIdentifierType", "SiteID");
				siteIdentifier.set("SystemIdentifierValue", String.valueOf(obj1.get("SiteID")));
				
				TableRow entityIdentifier = new TableRow();
				entityIdentifier.set("SystemIdentifierType", "EntityID");
				entityIdentifier.set("SystemIdentifierValue", String.valueOf(obj1.get("EntityID")));
				
				TableRow[] arr = new TableRow[2];
				arr[0] = siteIdentifier;
				arr[1] = entityIdentifier;
				
				op1.set("SystemIdentifier", arr);
				
				TableRow crosswalk = new TableRow();
				crosswalk.set("type", "configuration/sources/MDStaff");
				crosswalk.set("value", String.valueOf(obj1.get("SiteID")));
				
				op1.set("crosswalks", crosswalk);
				
				TableRow zip = new TableRow();
				zip.set("PostalCode", String.valueOf(obj1.get("PostalCode")));
				zip.set("Zip5", "");
				zip.set("Zip4", "");
				
				op1.set("Zip", zip);
				
				c.output(op1);
			}
		}));
		
		PCollection<TableRow> mergedData = PCollectionList.of(addressTransformed).and(siteTransformed)
	                .apply(Flatten.<TableRow>pCollections());
		
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); 

		fields.add(new TableFieldSchema().setName("AddressLine1").setType("String").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("AddressLine2").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("City").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("StateProvince").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("Country").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("SubAdministrativeArea").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("PublishToExternalDirectoryFlag").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("PublishToExternalDirectoryPrimaryFlag").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("AddressInUse").setType("STRING").setMode("NULLABLE"));
		fields.add(new TableFieldSchema().setName("SystemIdentifier").setType("STRUCT").setMode("REPEATED").setFields(
						Arrays.asList(new TableFieldSchema().setName("SystemIdentifierType").setType("String"),
						new TableFieldSchema().setName("SystemIdentifierValue").setType("STRING"))));
		fields.add(new TableFieldSchema().setName("crosswalks").setType("STRUCT").setFields(
						Arrays.asList(new TableFieldSchema().setName("type").setType("String"),
						new TableFieldSchema().setName("value").setType("STRING"))));
		fields.add(new TableFieldSchema().setName("Zip").setType("STRUCT").setFields(
				Arrays.asList(new TableFieldSchema().setName("PostalCode").setType("String"),
						new TableFieldSchema().setName("Zip5").setType("String"),
				new TableFieldSchema().setName("Zip4").setType("STRING"))));
						
		TableSchema schema = new TableSchema().setFields(fields);
		
		mergedData.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("asc-ahnat-apdh-sbx:apdh_test_dataset.aaaa")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
	
		pipeline.run(options).waitUntilFinish();
	}

}
