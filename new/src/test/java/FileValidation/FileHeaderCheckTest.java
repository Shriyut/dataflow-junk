package FileValidation;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.apache.beam.sdk.transforms.ParDo;

public class FileHeaderCheckTest {
	
	@Mock
	FileHeaderCheck FileHeaderCheckMock;
	
	@Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();
	
	@Test
    public void testValidHeader(){
		
		PCollection<String> output = testPipeline.apply("Create input", Create.of("gs://apdh-test-bucket/anotherfile.csv"))
				.apply(ParDo.of(new FileHeaderCheck()));
		
		PAssert.that(output).containsInAnyOrder("true");
		testPipeline.run().waitUntilFinish();
	}
	
	@Test
    public void testInValidFile(){
		
		PCollection<String> output = testPipeline.apply("Create input", Create.of("gs://apdh-test-bucket/anotherfile1.csv"))
				.apply(ParDo.of(new FileNameCheck()));
		
		PAssert.that(output).containsInAnyOrder("false");
		testPipeline.run().waitUntilFinish();
	}
	
	
}
