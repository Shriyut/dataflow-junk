package org.ascension.addg.gcp.pubsub;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;




public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String s = LocalDateTime.now().toString();
		
		LocalDateTime date = LocalDateTime.parse(s);
		System.out.println(date.toString());
		
		final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		
		String format = "yyyy-MM-dd";
		System.out.println(date.format(DateTimeFormatter.ofPattern(format)));
		
		String t = "2021-11-15 14:48:18.487023 UTC";
		LocalDateTime date2 = LocalDateTime.parse(t);
		System.out.println(date2.toString());
	}

}
