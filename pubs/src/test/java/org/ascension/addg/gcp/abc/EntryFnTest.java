package org.ascension.addg.gcp.abc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class EntryFnTest {

	@Rule
	public final transient TestPipeline testPipeline = TestPipeline.create();
	
	@Test
	public void abcEntryTest() {
		List<String> stages = new ArrayList<>();
		stages.add("BatchJobStart");
		stages.add("DeliveryLayerJobStart");
		stages.add("countEntry");
		stages.add("PubsubCountEntry");
		stages.add("Finished");
		
		StringBuilder result = new StringBuilder();
		
		stages.stream().forEach((k)->{
			TableRow checkTableRow = AbcEntry.getEntry(k, "sample");
			if(!checkTableRow.isEmpty()) {
				result.append("Y");
			}
		});
		
		assertEquals(result.toString(), "YYYYY");
	}
	
	@Test(expected=AssertionError.class)
	public void entryFnTest() {
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("description", "Job Started").set("src_system_name", "sample"));
		
		ImmutableList<TableRow> newInput = ImmutableList.of(new TableRow());
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		
		PCollection<TableRow> output = inputPC.apply(ParDo.of(new EntryFn("NotApplicable", "BatchJobStart", "sample", "NotApplicable")));
		
		PAssert.that(output).containsInAnyOrder(input);
		
		testPipeline.run().waitUntilFinish();
	}
	
	@Test(expected=RuntimeException.class)
	public void writeToAbcTableTest() {
		AbcEntry.writeToAbcTable(null, null, null, null, null, null);
	}
	
	@Test
	public void getTimeTest() {
		String time = AbcEntry.getTime();
		assertNotNull(time);
	}
}