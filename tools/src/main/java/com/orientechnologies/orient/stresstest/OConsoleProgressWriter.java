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
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.stresstest.workload.OWorkload;

/**
 * Takes care of updating the console with a completion percentage while the stress test is working;
 * it takes the data to show from the OStressTestResults class.
 *
 * @author Andrea Iacono
 */
public class OConsoleProgressWriter extends OSoftThread {

  private final OWorkload workload;
  private String lastResult = null;

  public OConsoleProgressWriter(String name, final OWorkload workload) {
    super(name);
    this.workload = workload;
  }

  public void printMessage(final String message) {
    System.out.println(message);
  }

  @Override
  protected void execute() throws Exception {
    final String result = workload.getPartialResult();
    if (lastResult == null || !lastResult.equals(result))
      System.out.print("\r- Workload in progress " + result);
    lastResult = result;
    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      softShutdown();
    }
  }

  @Override
  public void sendShutdown() {
    try {
      execute(); // flushes the final result, if we missed it
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.sendShutdown();
  }
}
