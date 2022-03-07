package streaming.test;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class test {

	public static void main(String[] args) throws ClientProtocolException, IOException {
		// TODO Auto-generated method stub

		HttpClient client = HttpClientBuilder.create().build();    
	    HttpResponse response = client.execute(new HttpGet("https://www.stackoverflow.com"));
	    int statusCode = response.getStatusLine().getStatusCode();
	    System.out.println(statusCode);
	    
	    String c = "{\r\n"
	    		+ "	\"val\": {\r\n"
	    		+ "		\"Code\": \"x\",\r\n"
	    		+ "	}\r\n"
	    		+ "\r\n"
	    		+ "}";
	    
	    Config cnf = ConfigFactory.parseString(c).resolve();
	    Config h = cnf.getConfig("val");
	    String j = h.getString("Code");
	}

}
