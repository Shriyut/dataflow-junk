package org.ascension.addg.gcp.mdstaff.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Instant;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.values.TypeDescriptor;

public class OrgTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);
		Pipeline pipeline = Pipeline.create(options);

		PCollection<TableRow> org = pipeline
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM mdstaff_ingest.appointment_land"));

		PCollection<TableRow> clinical1 = org.apply(ParDo.of(new ClinicalFn("ClinicalExpertise1_Code")));
		PCollection<TableRow> clinical2 = org.apply(ParDo.of(new ClinicalFn("ClinicalExpertise2_Code")));
		PCollection<TableRow> clinical3 = org.apply(ParDo.of(new ClinicalFn("ClinicalExpertise3_Code")));
		PCollection<TableRow> clinical4 = org.apply(ParDo.of(new ClinicalFn("ClinicalExpertise4_Code")));

		PCollection<TableRow> mergedData = PCollectionList.of(clinical1).and(clinical2).and(clinical3).and(clinical4)
				.apply(Flatten.<TableRow>pCollections());
		

		PCollection<TableRow> asdf = mergedData
				.apply(Distinct.withRepresentativeValueFn((TableRow tr) -> (String) tr.get("ClinicalExpertise"))
						.withRepresentativeType(TypeDescriptor.of(String.class)));
		
		PCollection<TableRow> finaltr = asdf.apply(ParDo.of(new DoFn<TableRow, TableRow>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) {
				TableRow obj = c.element().clone();
				
				TableRow output = new TableRow();
				TableRow crosswalk = new TableRow();
				
				crosswalk.set("type", "configuration/sources/MDStaff");
				crosswalk.set("value", String.valueOf(obj.get("ClinicalExpertise")));
				
				output.set("ClinicalExpertise", String.valueOf(obj.get("ClinicalExpertise")));
				output.set("crosswalks", crosswalk);
				
				c.output(output);
			}
		}));
		  List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(); //c //
		  
			fields.add(new TableFieldSchema().setName("ClinicalExpertise").setType("String").setMode("NULLABLE"));
			fields.add(new TableFieldSchema().setName("crosswalks").setType("STRUCT").setFields(
					Arrays.asList(new TableFieldSchema().setName("type").setType("String"),
					new TableFieldSchema().setName("value").setType("STRING"))));
			TableSchema schema = new TableSchema().setFields(fields);
		  
			finaltr.apply("Writing to BQ",
					BigQueryIO.writeTableRows().to("asc-ahnat-apdh-sbx:apdh_test_dataset.aaclnical")
							.withSchema(schema)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		 
		pipeline.run(options).waitUntilFinish();
	}

	public static class ClinicalFn extends DoFn<TableRow, TableRow> {

		private String key;

		public ClinicalFn(String key) {
			this.key = key;
		}

		// List<String> values = new ArrayList<>();
		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			TableRow obj = c.element().clone();
			// System.out.println("ClinicalFn"+c.element().toString());
			TableRow transformedValue = new TableRow();
			// String x = obj.get(key);
			if (obj.get(key) != null) {
				// if(!x.equals(null)) {
				String clinicalValue = String.valueOf(obj.get(key));
				System.out.println("keyval" + String.valueOf(obj.get(key)));
				System.out.println("notequal" + String.valueOf(obj.get(key) != null));
				System.out.println("equals" + String.valueOf(obj.get(key).equals(null)));
				transformedValue.set("ClinicalExpertise", clinicalValue);
				System.out.println(key + "tablerow" + transformedValue.toString());
				c.output(transformedValue);
			}

		}
	}

	public static class RemoveDuplicateFn extends DoFn<TableRow, TableRow> {

		List<String> values = new ArrayList<>();

		@ProcessElement
		public void ProcessElement(ProcessContext c) {
			TableRow obj = c.element().clone();

			obj.entrySet().forEach((k) -> {
				String val = String.valueOf(k.getValue());
				values.add(val);
			});
			// System.out.println(values.size());

		}

		@FinishBundle
		public void FinishBundle(FinishBundleContext fbc) {
			TableRow finalOutput = new TableRow();
			Set<String> nonDuplicateValues = new LinkedHashSet<String>(values);
			nonDuplicateValues.forEach((k) -> {
				finalOutput.set("ClinicalExpertise", k);
			});
			System.out.println(nonDuplicateValues.size());
			fbc.output(finalOutput, Instant.now(), GlobalWindow.INSTANCE);
		}
	}

	/*
	 * public class CV extends SimpleFunction<TableRow, List<String>>{
	 * 
	 * public List<String> apply(TableRow input){ List<String> uio = new
	 * ArrayList<>(); input.forEach((k, v)->{ uio.add(String.valueOf(v)); }); } }
	 */

}
