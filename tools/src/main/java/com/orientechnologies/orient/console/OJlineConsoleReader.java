package com.orientechnologies.orient.console;

import java.io.IOException;
import java.io.Writer;

import jline.console.ConsoleReader;

import com.orientechnologies.common.console.OConsoleApplication;
import com.orientechnologies.common.console.OConsoleReader;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/10/13
 */
public class OJlineConsoleReader implements OConsoleReader {
  private final ConsoleReader consoleReader;

  public OJlineConsoleReader() {
    try {
      consoleReader = new ConsoleReader();
    } catch (IOException e) {
      throw new IllegalStateException("Error during console reader initialization", e);
    }
  }

  @Override
  public String readLine() {
    try {
      return consoleReader.readLine();
    } catch (IOException e) {
      throw new IllegalStateException("Error during reading of user input", e);
    }
  }

  @Override
  public String readLine(String prompt) {
    try {
      return consoleReader.readLine(prompt);
    } catch (IOException e) {
      throw new IllegalStateException("Error during reading of user input", e);
    }
  }

  @Override
  public void setPrompt(String prompt) {
    consoleReader.setPrompt(prompt);
  }

  @Override
  public Writer getOut() {
    return consoleReader.getOutput();
  }

  @Override
  public Writer getErr() {
    return consoleReader.getOutput();
  }

  @Override
  public void setConsole(OConsoleApplication console) {
  }

  @Override
  public OConsoleApplication getConsole() {
    return null;
  }

  @Override
  public boolean hasPromptSupport() {
    return true;
  }
}
