package org.ascension.addg.gcp.pubsub;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.net.ssl.*" })
public class UpdateStatefileTimestampTest {

	@Rule
	public final transient TestPipeline testPipeline = TestPipeline.create();
	
	
	UpdateStatefileTimestamp ust = mock(UpdateStatefileTimestamp.class);
	
	@Test
	public void updateCheck() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime utcDateTime = now.withZoneSameInstant(ZoneId.of("UTC"));
		
		String newTime = dtf.format(utcDateTime);
		
		String timestampKey = "last_pull_timestamp";
		
		String gcsPath = "gs://samplePath";
		
		String input =  "[{"
				+ "\"col1\": \"val1\","
				+ "\"last_pull_timestamp\": \"2020-01-01\","
				+ "\"col2\": \"val2\""
				+ "}]";
		
		PCollection<String> inputPC = testPipeline.apply(Create.of(input));
		
		PCollection<String> outputPC = inputPC.apply(ParDo.of(new UpdateStatefileTimestamp(newTime, timestampKey)));
		
		String output = "[{"
				+ "\"col1\": \"val1\","
				+ "\"last_pull_timestmap\": "+"\""+newTime+"\","
				+ "\"col2\": \"val2\""
				+ "}]";
		String op = "[{\"last_pull_timestamp\":\""+newTime+"\",\"col2\":\"val2\",\"col1\":\"val1\"}]";
		PAssert.that(outputPC).containsInAnyOrder(op);
		
		testPipeline.run().waitUntilFinish();
		
		
	}
	
	@Test(expected = Exception.class)
	public void callUpdateTimstampTest() {
		UpdateStatefileTimestamp.callUpdateTimestamp(null, null, null, null, null);
	}
	
	@Test
	public void callTest() {
		PCollection<String> sample = testPipeline.apply("Creating test PCollection", Create.of("sample"));
		PCollection<Long> count = sample.apply(Count.globally());
		PCollectionView<Long> view = count.apply(View.asSingleton());
		doNothing().when(ust).callUpdateTimestamp(sample, "test", "test", "test", view);
		UpdateStatefileTimestamp.callUpdateTimestamp(sample, "test", "test", "test", view);
		verify(ust,times(1)).callUpdateTimestamp(sample, "test", "test", "test", view);
		
	}
	
	@Test
	public void voidMethodTest() {
		PCollection<String> sample = testPipeline.apply("Creating test PCollection", Create.of("sample"));
		PCollection<Long> count = sample.apply(Count.globally());
		PCollectionView<Long> view = count.apply(View.asSingleton());
		try(MockedStatic<UpdateStatefileTimestamp> ust = mockStatic(UpdateStatefileTimestamp.class)){
			ust.when(()-> UpdateStatefileTimestamp.callUpdateTimestamp(sample, "test", "test", "test", view)).thenAnswer(new Answer<Object>() {
				public Object answer(InvocationOnMock invocationOnMock) throws Throwable{
					return null;
				}
			});
		}
	}
	
	@Test
	public void update() {
		PCollection<String> sample = testPipeline.apply("Creating test PCollection", Create.of("sample"));
		PCollection<Long> count = sample.apply(Count.globally());
		PCollectionView<Long> view = count.apply(View.asSingleton());
		
		UpdateStatefileTimestamp.callUpdateTimestamp(sample, "test", "test", "test", view);
		testPipeline.run().waitUntilFinish();
	}
	
}
