package com.orientechnologies.common.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author <a href='mailto:enisher@gmail.com'> Artem Orobets </a>
 */
public class OScannerCommandStream implements OCommandStream {
  private Scanner scanner;

  public OScannerCommandStream(String commands) {
    scanner = new Scanner(commands);
    init();
  }

  public OScannerCommandStream(File file) throws FileNotFoundException {
    scanner = new Scanner(file);
    init();
  }

  private void init() {
    scanner.useDelimiter(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?=([^']*'[^']*')*[^']*$)|\n");
  }

  @Override
  public boolean hasNext() {
    return scanner.hasNext();
  }

  @Override
  public String nextCommand() {
    return scanner.next().trim();
  }

  @Override
  public void close() {
    scanner.close();
  }
}
