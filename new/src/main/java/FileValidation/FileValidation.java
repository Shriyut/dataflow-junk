package FileValidation;

import java.io.IOException;

import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.api.services.dataflow.model.Job;

public class FileValidation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		//options.setProject("spry-abacus-325317");
		//options.setTempLocation("gs://dataflow_test1234/temp");
		//options.setStagingLocation("gs://dataflow_test1234/output");
		//options.setRunner(DataflowRunner.class);
		//options.setRegion("us-central1");
		//Pipeline pipeline = Pipeline.create(options);
		
		DataflowPipelineOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		
		//Each file validation check will either return true or false
		String inputFile = Config.fileName;
		PCollection<String> file = pipeline.apply("Creating file PCollection", Create.of(inputFile));
		PCollection<String> fileNameCheck =	file.apply(ParDo.of(new FileNameCheck()));
				
		PCollection<String> fileHeaderCheck = file.apply(ParDo.of(new FileHeaderCheck()));
				
		PCollection<String> recordCountCheck = file.apply(ParDo.of(new RecordCountCheck()));
		
		PCollection<String> timeDiffCheck = file.apply(ParDo.of(new TimeDiffCheck()));
		
		//String path = "gs://apdh-test-bucket/data/*"; 
		String path = Config.bucketPath;
		PCollection<String> fileCountCheck = pipeline.apply(FileIO.match().filepattern(path))
		.apply(FileIO.readMatches())
		.apply(ParDo.of(new FileCountCheck()));
		
		//Combining all responses together
		PCollectionList<String> values = PCollectionList.of(fileNameCheck).and(fileHeaderCheck).and(recordCountCheck).and(timeDiffCheck).and(fileCountCheck);
		PCollection<String> merged = values.apply(Flatten.<String>pCollections());
		PCollection<String> distinct = merged.apply(Distinct.<String>create());
		
		distinct.apply(ParDo.of(new DoFn<String, String>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c) throws IOException {
				if(c.element().equals("false")) {
					//Cancels the pipeline if any of the file validation steps return false
					PipelineOptions options = c.getPipelineOptions();
					DataflowPipelineOptions dfoptions = options.as(DataflowPipelineOptions.class);
					
					DataflowWorkerHarnessOptions dwhoptions = options.as(DataflowWorkerHarnessOptions.class);
					
					String jobId = dwhoptions.getJobId();
					DataflowClient dataflowClient = DataflowClient.create(dfoptions);
					Job jobDescription = dataflowClient.getJob(jobId);
					jobDescription.setRequestedState("JOB_STATE_CANCELLED");
					dataflowClient.updateJob(jobId, jobDescription);
					
				}
			}
		}));
		
		pipeline.run(options).waitUntilFinish();
	}

}
