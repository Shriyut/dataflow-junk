package reject;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;



import org.apache.beam.sdk.transforms.ParDo;

public class RejectTest {
	@Mock
	Reject RejectMock;
	
	@Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
	
	@Test
    public void testValidFile(){
		
		ImmutableList<TableRow> input = ImmutableList.of(new TableRow().set("run_id", "testVal1").set("run_error", "testVal2"));
		PCollection<TableRow> inputPC = testPipeline.apply(Create.<TableRow>of(input));
		PCollection<TableRow> output = inputPC.apply(ParDo.of(new Reject()));
		
		TableRow outputTableRow = new TableRow();
		outputTableRow.set("reject_record", "testVal1|testVal2|");

		PAssert.that(output).containsInAnyOrder(outputTableRow);

		testPipeline.run().waitUntilFinish();
	}
}