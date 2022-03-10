package FileValidation;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.apache.beam.sdk.transforms.ParDo;

public class FileCountCheckTest {
	@Mock
	FileCountCheck FileCountCheckMock;
	
	@Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
	
	@Test
    public void testValidFileCount(){
		
		String path = "gs://apdh-test-bucket/data/*"; 
		PCollection<String> output = testPipeline.apply(FileIO.match().filepattern(path))
		.apply(FileIO.readMatches())
		.apply(ParDo.of(new FileCountCheck()));
		
		PAssert.that(output).containsInAnyOrder("true");
		testPipeline.run().waitUntilFinish();
	}
}
