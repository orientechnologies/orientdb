package com.orientechnologies.common.console;

import java.io.IOException;

public interface OConsoleReader {
  public String readLine() throws IOException;

  public void setConsole(OConsoleApplication console);

  public OConsoleApplication getConsole();
}
