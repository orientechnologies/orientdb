package com.orientechnologies.common.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DefaultConsoleReader implements OConsoleReader {
	final BufferedReader	reader	= new BufferedReader(new InputStreamReader(System.in));

	public String readLine() {
		try {
			return reader.readLine();
		} catch (IOException e) {
			return null;
		}
	}

	public OConsoleApplication getConsole() {
		return null;
	}

	public void setConsole(OConsoleApplication console) {
	}
}
