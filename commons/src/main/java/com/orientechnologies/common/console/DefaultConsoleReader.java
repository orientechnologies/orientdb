package com.orientechnologies.common.console;

import java.io.*;

public class DefaultConsoleReader implements OConsoleReader {
  final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

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

  @Override
  public boolean hasPromptSupport() {
    return false;
  }

  @Override
  public String readLine(String prompt) {
    throw new UnsupportedOperationException("readLine(prompt)");
  }

  @Override
  public void setPrompt(String prompt) {
  }

  @Override
  public Writer getOut() {
    return new OutputStreamWriter(System.out);
  }

  @Override
  public Writer getErr() {
    return new OutputStreamWriter(System.err);
  }
}
