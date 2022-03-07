package org.ascension.addg.gcp.pubsub;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HcoTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		PCollection<TableRow> facility = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.facility_land LIMIT 100"));
		PCollection<TableRow> entity = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.entity_land LIMIT 100"));
		PCollection<TableRow> site = pipeline.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.site_land LIMIT 100"));
		
		PCollection<TableRow> facilityTransformed = facility.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				
				TableRow facility = new TableRow();
				
				facility.set("OrganizationName", String.valueOf(obj.get("Comments")));
				facility.set("OrganizationTypeCode", "Facility");
				facility.set("OrganizationCode", String.valueOf(obj.get("Code")));
				facility.set("Status", "");
				facility.set("EntityInUseFlag", "");
				facility.set("SiteInUseFlag", "");
				facility.set("FacilityArchivedFlag", String.valueOf(obj.get("Archived")));
				facility.set("DoingBusinessAs", "");
				facility.set("PatientCenteredMedicalHomeFlag", "");
				
				TableRow crosswalks = new TableRow();
				crosswalks.set("type", "configuration/sources/MDStaff");
				crosswalks.set("value", String.valueOf(obj.get("FacilityID")));
				
				facility.set("crosswalks", crosswalks);
				
				TableRow managedIdentifier = new TableRow();
				managedIdentifier.set("ManagedIdentifierType", "");
				managedIdentifier.set("ManagedIdentifierValue", "");
				managedIdentifier.set("IssuingAuthority", "");
				managedIdentifier.set("State", "");
				managedIdentifier.set("InitialIssueDate", "");
				managedIdentifier.set("EffectiveDate", "");
				managedIdentifier.set("RenewalDate", "");
				managedIdentifier.set("ExpirationDate", "");
				
				facility.set("ManagedIdentifier", managedIdentifier);
				
				TableRow systemIdentifier = new TableRow();
				systemIdentifier.set("SystemIdentifierType", "FacilityID");
				systemIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("FacilityID")));
				
				facility.set("SystemIdentifier", systemIdentifier);
				
				TableRow email = new TableRow();
				email.set("EmailType", "");
				email.set("Email", "");
				
				facility.set("Email", email);
				
				c.output(facility);
			}
		}));
		
		PCollection<TableRow> entityTransformed = entity.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				
				TableRow entity = new TableRow();
				
				entity.set("OrganizationName", String.valueOf(obj.get("Name")));
				entity.set("OrganizationTypeCode", "Entity");
				entity.set("OrganizationCode", "");
				entity.set("Status", "");
				entity.set("EntityInUseFlag", String.valueOf(obj.get("InUse")));
				entity.set("SiteInUseFlag", "");
				entity.set("FacilityArchivedFlag", "");
				entity.set("DoingBusinessAs", "");
				entity.set("PatientCenteredMedicalHomeFlag", "");
				
				TableRow crosswalks = new TableRow();
				crosswalks.set("type", "configuration/sources/MDStaff");
				crosswalks.set("value", String.valueOf(obj.get("EntityID")));
				
				TableRow[] managedIdentifier = new TableRow[4];
				TableRow providerIdentifier = new TableRow();
				providerIdentifier.set("ManagedIdentifierType", "National Provider Identifier");
				providerIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("NPI")));
				providerIdentifier.set("IssuingAuthority", "");
				providerIdentifier.set("State", "");
				providerIdentifier.set("InitialIssueDate", "");
				providerIdentifier.set("EffectiveDate", "");
				providerIdentifier.set("RenewalDate", "");
				providerIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[0] = providerIdentifier;
				
				TableRow transactionIdentifier = new TableRow();
				transactionIdentifier.set("ManagedIdentifierType", "Provider Transaction Access Number");
				transactionIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("MedicareNumber")));
				transactionIdentifier.set("IssuingAuthority", "");
				transactionIdentifier.set("State", "");
				transactionIdentifier.set("InitialIssueDate", "");
				transactionIdentifier.set("EffectiveDate", "");
				transactionIdentifier.set("RenewalDate", "");
				transactionIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[1] = transactionIdentifier;
				
				TableRow medicaidIdentifier = new TableRow();
				medicaidIdentifier.set("ManagedIdentifierType", "Medicaid Number");
				medicaidIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("MedicaidNumber")));
				medicaidIdentifier.set("IssuingAuthority", "");
				medicaidIdentifier.set("State", "");
				medicaidIdentifier.set("InitialIssueDate", "");
				medicaidIdentifier.set("EffectiveDate", "");
				medicaidIdentifier.set("RenewalDate", "");
				medicaidIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[2] = medicaidIdentifier;
				
				TableRow taxPayerIdentifier = new TableRow();
				taxPayerIdentifier.set("ManagedIdentifierType", "Taxpayer Identification Number");
				taxPayerIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("TaxIDNumber")));
				taxPayerIdentifier.set("IssuingAuthority", "");
				taxPayerIdentifier.set("State", "");
				taxPayerIdentifier.set("InitialIssueDate", "");
				taxPayerIdentifier.set("EffectiveDate", "");
				taxPayerIdentifier.set("RenewalDate", "");
				taxPayerIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[3] = taxPayerIdentifier;
				
				entity.set("ManagedIdentifier", managedIdentifier);
				
				TableRow parentEntityIdentifier = new TableRow();
				parentEntityIdentifier.set("SystemIdentifierType", "ParentEntityID");
				parentEntityIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("ParentEntityID")));
				
				TableRow entityIdentifier = new TableRow();
				entityIdentifier.set("SystemIdentifierType", "EntityID");
				entityIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("EntityID")));
				
				TableRow[] systemIdentifier = new TableRow[2];
				systemIdentifier[0] = parentEntityIdentifier;
				systemIdentifier[1] = entityIdentifier;
				
				entity.set("SystemIdentifier", systemIdentifier);
				
				TableRow email = new TableRow();
				email.set("EmailType", "");
				email.set("Email", "");
				
				entity.set("Email", email);
				
				c.output(entity);
			}
		}));
		
		PCollection<TableRow> siteTransformed = site.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				
				TableRow site = new TableRow();
				
				site.set("OrganizationName", String.valueOf(obj.get("Name")));
				site.set("OrganizationTypeCode", String.valueOf(obj.get("SiteType_Code")));
				site.set("OrganizationCode", "");
				site.set("Status", "");
				site.set("EntityInUseFlag", "");
				site.set("SiteInUseFlag", String.valueOf(obj.get("InUse")));
				site.set("FacilityArchivedFlag", "");
				site.set("DoingBusinessAs", "");
				site.set("PatientCenteredMedicalHomeFlag", String.valueOf(obj.get("PCMHID_Code")));
				
				TableRow[] managedIdentifier = new TableRow[4];
				TableRow providerIdentifier = new TableRow();
				providerIdentifier.set("ManagedIdentifierType", "National Provider Identifier");
				providerIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("NPI")));
				providerIdentifier.set("IssuingAuthority", "");
				providerIdentifier.set("State", "");
				providerIdentifier.set("InitialIssueDate", "");
				providerIdentifier.set("EffectiveDate", "");
				providerIdentifier.set("RenewalDate", "");
				providerIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[0] = providerIdentifier;
				
				TableRow transactionIdentifier = new TableRow();
				transactionIdentifier.set("ManagedIdentifierType", "Provider Transaction Access Number");
				transactionIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("MedicareNumber")));
				transactionIdentifier.set("IssuingAuthority", "");
				transactionIdentifier.set("State", "");
				transactionIdentifier.set("InitialIssueDate", "");
				transactionIdentifier.set("EffectiveDate", "");
				transactionIdentifier.set("RenewalDate", "");
				transactionIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[1] = transactionIdentifier;
				
				TableRow medicaidIdentifier = new TableRow();
				medicaidIdentifier.set("ManagedIdentifierType", "Medicaid Number");
				medicaidIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("MedicaidNumber")));
				medicaidIdentifier.set("IssuingAuthority", "");
				medicaidIdentifier.set("State", "");
				medicaidIdentifier.set("InitialIssueDate", "");
				medicaidIdentifier.set("EffectiveDate", "");
				medicaidIdentifier.set("RenewalDate", "");
				medicaidIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[2] = medicaidIdentifier;
				
				TableRow taxPayerIdentifier = new TableRow();
				taxPayerIdentifier.set("ManagedIdentifierType", "Taxpayer Identification Number");
				taxPayerIdentifier.set("ManagedIdentifierValue", String.valueOf(obj.get("TaxIDNumber")));
				taxPayerIdentifier.set("IssuingAuthority", "");
				taxPayerIdentifier.set("State", "");
				taxPayerIdentifier.set("InitialIssueDate", "");
				taxPayerIdentifier.set("EffectiveDate", "");
				taxPayerIdentifier.set("RenewalDate", "");
				taxPayerIdentifier.set("ExpirationDate", "");
				
				managedIdentifier[3] = taxPayerIdentifier;
				
				site.set("ManagedIdentifier", managedIdentifier);
				
				TableRow siteIdentifier = new TableRow();
				siteIdentifier.set("SystemIdentifierType", "SiteID");
				siteIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("SiteID")));
				
				TableRow entityIdentifier = new TableRow();
				entityIdentifier.set("SystemIdentifierType", "EntityID");
				entityIdentifier.set("SystemIdentifierValue", String.valueOf(obj.get("EntityID")));
				
				TableRow[] systemIdentifier = new TableRow[2];
				systemIdentifier[0] = siteIdentifier;
				systemIdentifier[1] = entityIdentifier;
				
				site.set("SystemIdentifier", systemIdentifier);
				
				TableRow crosswalk = new TableRow();
				crosswalk.set("type", "configuration/sources/MDStaff");
				crosswalk.set("value", String.valueOf(obj.get("SiteID")));
				
				site.set("crosswalks", crosswalk);
				
				TableRow email = new TableRow();
				email.set("EmailType", "");
				email.set("Email", "");
				
				site.set("Email", email);
				
				c.output(site);
			}
		}));
		
		PCollection<TableRow> mergedData = PCollectionList.of(facilityTransformed).and(entityTransformed).and(siteTransformed)
                .apply(Flatten.<TableRow>pCollections());
		
		List<TableFieldSchema> fieldsOrg = new ArrayList<TableFieldSchema>(); 

		fieldsOrg.add(new TableFieldSchema().setName("OrganizationName").setType("String").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("OrganizationTypeCode").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("OrganizationCode").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("Status").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("EntityInUseFlag").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("SiteInUseFlag").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("FacilityArchivedFlag").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("DoingBusinessAs").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("PatientCenteredMedicalHomeFlag").setType("STRING").setMode("NULLABLE"));
		fieldsOrg.add(new TableFieldSchema().setName("SystemIdentifier").setType("STRUCT").setMode("REPEATED").setFields(
						Arrays.asList(new TableFieldSchema().setName("SystemIdentifierType").setType("String"),
						new TableFieldSchema().setName("SystemIdentifierValue").setType("STRING"))));
		fieldsOrg.add(new TableFieldSchema().setName("crosswalks").setType("STRUCT").setFields(
						Arrays.asList(new TableFieldSchema().setName("type").setType("String"),
						new TableFieldSchema().setName("value").setType("STRING"))));
		fieldsOrg.add(new TableFieldSchema().setName("ManagedIdentifier").setType("STRUCT").setMode("REPEATED").setFields(
				Arrays.asList(new TableFieldSchema().setName("ManagedIdentifierType").setType("String"),
						new TableFieldSchema().setName("ManagedIdentifierValue").setType("String"),
				new TableFieldSchema().setName("IssuingAuthority").setType("STRING"),
				new TableFieldSchema().setName("State").setType("STRING"),
				new TableFieldSchema().setName("InitialIssueDate").setType("STRING"),
				new TableFieldSchema().setName("EffectiveDate").setType("STRING"),
				new TableFieldSchema().setName("RenewalDate").setType("STRING"),
				new TableFieldSchema().setName("ExpirationDate").setType("STRING"))));
		fieldsOrg.add(new TableFieldSchema().setName("Email").setType("STRUCT").setFields(
						Arrays.asList(new TableFieldSchema().setName("EmailType").setType("String"),
						new TableFieldSchema().setName("Email").setType("STRING"))));		

						
		TableSchema schemaOrg = new TableSchema().setFields(fieldsOrg);
		
		mergedData.apply("Writing to BQ", BigQueryIO.writeTableRows()
				.to("asc-ahnat-apdh-sbx:apdh_test_dataset.aaOrgData")
				.withSchema(schemaOrg)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		
		pipeline.run(options).waitUntilFinish();
	}

}
