package com.orientechnologies.common.console;

import java.io.Writer;

public interface OConsoleReader {
  public String readLine();

  public String readLine(String prompt);

  public void setConsole(OConsoleApplication console);

  public OConsoleApplication getConsole();

  public boolean hasPromptSupport();

  public void setPrompt(String prompt);

  public Writer getOut();

  public Writer getErr();
}
