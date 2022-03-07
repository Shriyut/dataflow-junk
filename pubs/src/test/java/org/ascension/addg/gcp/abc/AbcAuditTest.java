package org.ascension.addg.gcp.abc;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;

public class AbcAuditTest {
	@Rule
	public final transient TestPipeline testPipeline = TestPipeline.create();
	
	
	@Mock
	AbcAudit AbcAuditMock;
	
	@Test(expected = RuntimeException.class)
	public void WriteTest() {
		
		//Tests if the pipeline fails when correct arguments aren't supplied to it
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("run_id", "Sample"));
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		
		String table = "invalidtablelocation";
		inputPC.apply(new AbcAudit(table));
		
		
		testPipeline.run().waitUntilFinish();
		
	}
}
