package org.ascension.addg.gcp.pubsub;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.PCollection;

public class PublishtoPubsub extends PTransform<PCollection<PubsubMessage>, PDone>{
	
	//Class to publish messages to PubSub 
	
	//Topic name is passed to the constructor of the class
	private String topicName;
	public PublishtoPubsub(String topicName) { 
		this.topicName = topicName; 
		}
	
	@Override
	public PDone expand(PCollection<PubsubMessage> input) {
		try {
		input.apply(PubsubIO.writeMessages().to(topicName)); 
		}catch(Exception e){
			e.printStackTrace();
		}
		return PDone.in(input.getPipeline()); 
		}
		
	
}
