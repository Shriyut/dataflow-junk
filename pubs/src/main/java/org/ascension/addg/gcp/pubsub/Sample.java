package org.ascension.addg.gcp.pubsub;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;

import org.ascension.addg.gcp.abc.*;


import com.google.api.services.bigquery.model.TableRow;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;


public class Sample {

	public static void main(String[] args) throws IOException {
		SampleIngestionOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(SampleIngestionOptions.class);
		Pipeline pipeline= Pipeline.create(options);
		
		
		String configUrl = options.getPipelineConfig().get();
		
		String fileContent = ReadConfig.readConf(configUrl);
		
		
			
		Config config = ConfigFactory.parseString(fileContent).resolve();
		Config pubsubConfig = config.getConfig("pubsubConfig");
		Config stateConfig = config.getConfig("statefile");
		
		String topicName = pubsubConfig.getString("topic_name");
		String watermark = pubsubConfig.getString("sourceWatermark");
		String val = stateConfig.getString("fileName");
		
		String gcsPath = options.getStatefileGcsPath().get();
		//String gcsFileName = gcsPath+val;
		
        String timestampKey = stateConfig.getString("timestamp");
        
        
        
		TopicPath topicPath = PubsubClient.topicPathFromName(options.getProject(), topicName);
		String topic=topicPath.getPath();
		
		PCollection<TableRow> rows = pipeline.apply(BigQueryIO.readTableRows()
				  .fromQuery("SELECT Property_Name, Street_Address, Zip_Code, City FROM [spry-abacus-325317.sourcedata.medxcel_source_data] WHERE id_property>2250"));		
		//extract topic path from config
		PCollection<PubsubMessage> sendData = rows.apply("sending data", ParDo.of(new CreatePubsubMessage(watermark)));
		sendData.apply(new PublishtoPubsub(topic));
		
		//AbcEntry.writeToAbcTable(rows, "NotApplicable", "BatchJobStart", "apdh_test.abcnew", "Symedical", "NotApplicable");
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime utcDateTime = now.withZoneSameInstant(ZoneId.of("UTC"));
		
		String newTime = dtf.format(utcDateTime);
		
		//ResourceId xx = FileSystems.matchNewResource(gcsFileName, false);
		//try {
		
		StringBuilder stateFileValidation = new StringBuilder();
		
		String gcsFileName = gcsPath+val;
		PCollection<Long> ghj = sendData.apply(Count.globally());
		PCollectionView<Long> count = ghj.apply(View.asSingleton());
		try {
			
			StringBuilder dataBuilder2 = new StringBuilder();
	        BufferedReader stateReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(gcsFileName, false))), "UTF-8"));
	        String line1;
	        while (( line1 = stateReader.readLine()) != null) {
	          dataBuilder2.append(line1);
	          dataBuilder2.append("\n");
	        }
	        
	        String stateContent = dataBuilder2.toString();
			FileSystems.delete(Arrays.asList(FileSystems.matchNewResource(gcsFileName, false)), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
			UpdateStatefileTimestamp.callUpdateTimestamp(pipeline.apply(Create.of(stateContent)), newTime, timestampKey, gcsFileName, count);
		}catch(FileNotFoundException e) {
			System.out.println("file not found");
			Config statefileContent = config.getConfig("statefileBackup");
			String content = statefileContent.getString("content");
			UpdateStatefileTimestamp.callUpdateTimestamp(pipeline.apply(Create.of(content)), newTime, timestampKey, gcsFileName, count);
		}
		
		
		//AbcEntry.writeToAbcTable(input, "NotApplicable", "BatchJobStart", "apdh_test.abcnew", "Symedical", "NotApplicable");
		
		pipeline.run(options).waitUntilFinish();
		
	}
	
	
}
