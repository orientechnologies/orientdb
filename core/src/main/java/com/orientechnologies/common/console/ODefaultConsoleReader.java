/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.common.console;

import com.orientechnologies.common.thread.OSoftThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Console reader implementation that uses the Java System.in.
 */
public class ODefaultConsoleReader implements OConsoleReader {
  final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  private class EraserThread extends OSoftThread {
    @Override
    protected void execute() throws Exception {
      System.out.print("\010*");
      try {
        Thread.currentThread().sleep(1);
      } catch (InterruptedException ie) {
      }
    }
  }

  @Override
  public String readLine() {
    try {
      return reader.readLine();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public String readPassword() {
    if (System.console() == null)
      // IDE
      return readLine();

    System.out.print(" ");

    final EraserThread et = new EraserThread();
    et.start();

    try {
      return reader.readLine();
    } catch (IOException e) {
      return null;
    } finally {
      et.sendShutdown();
    }
  }

  public OConsoleApplication getConsole() {
    return null;
  }

  public void setConsole(OConsoleApplication console) {
  }
}
