package streaming.test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import com.google.api.services.bigquery.model.TableRow;

public class fghjk {

	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		/*
		String s = "ioptre";
		byte[] a = s.getBytes("utf-8");
		
		String h = new String(a, StandardCharsets.UTF_8);
		System.out.println(h);
		*/
		
		boolean x = true;
		//System.out.println(x.toString());
		
		TableRow tr = new TableRow();
		tr.set("x", "y");
		Object y = tr.get("x");
		//y InstanceOf boolean;
		System.out.println(y.getClass().getName());
		
		//System.out.println(tr.get("x"));
	}

}
