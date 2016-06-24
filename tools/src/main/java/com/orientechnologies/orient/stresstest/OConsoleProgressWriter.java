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
package com.orientechnologies.orient.stresstest;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.stresstest.workload.OWorkload;

/**
 * Takes care of updating the console with a completion percentage while the stress test is working; it takes the data to show from
 * the OStressTestResults class.
 *
 * @author Andrea Iacono
 */
public class OConsoleProgressWriter extends OSoftThread {

  final private OWorkload workload;

  public OConsoleProgressWriter(final OWorkload workload) {
    this.workload = workload;
  }

  public void printMessage(final String message) {
    System.out.println(message);
  }

  @Override
  protected void execute() throws Exception {
    final String result = workload.getPartialResult();
    if (result != null)
      System.out.print("\rStress test in progress " + result);
  }
}
