package com.orientechnologies.common.console;

public interface OConsoleReader {
	public String readLine();

	public void setConsole(OConsoleApplication console);

	public OConsoleApplication getConsole();
}
