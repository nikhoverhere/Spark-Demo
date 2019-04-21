package com.nikh.digithon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {

	public static void main(String[] args) {
		
		Date date = new Date();
		System.out.println(date);
		DateFormat dateFormat = new SimpleDateFormat();
		String formattedDate= dateFormat.format(date);
		System.out.println(formattedDate);

	}

}
