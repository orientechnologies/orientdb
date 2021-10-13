/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.console;

import com.orientechnologies.common.thread.OSoftThread;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** Console reader implementation that uses the Java System.in. */
public class ODefaultConsoleReader implements OConsoleReader {
  private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  private static class EraserThread extends OSoftThread {
    public EraserThread(String name) {
      super(name);
    }

    @Override
    @SuppressWarnings({"checkstyle:AvoidEscapedUnicodeCharacters", "checkstyle:IllegalTokenText"})
    protected void execute() throws Exception {
      System.out.print("\u0008*");
      try {
        Thread.sleep(1);
      } catch (InterruptedException ignore) {
        // om nom nom
      }
    }
  }

  @Override
  public String readLine() {
    try {
      return reader.readLine();
    } catch (IOException ignore) {
      return null;
    }
  }

  @Override
  public String readPassword() {
    if (System.console() == null)
      // IDE
      return readLine();

    System.out.print(" ");

    final EraserThread et = new EraserThread("Read password thread");
    et.start();

    try {
      return reader.readLine();
    } catch (IOException ignore) {
      return null;
    } finally {
      et.sendShutdown();
    }
  }

  @Override
  public void setConsole(OConsoleApplication console) {}

  @Override
  public int getConsoleWidth() {
    return OConsoleReader.FALLBACK_CONSOLE_WIDTH;
  }
}
