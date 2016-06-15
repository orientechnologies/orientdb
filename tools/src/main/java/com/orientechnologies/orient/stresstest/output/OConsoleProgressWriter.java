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
package com.orientechnologies.orient.stresstest.output;

import com.orientechnologies.orient.stresstest.operations.OOperationType;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Takes care of updating the console with a completion percentage
 * while the stress test is working; it takes the data to show
 * from the OStressTestResults class.
 *
 * @author Andrea Iacono
 */
public class OConsoleProgressWriter implements Runnable {

  final private OOperationsSet     operationsSet;
  final private OStressTestResults results;
  final private int                threadsNumber;

  private boolean isRunning       = true;
  private int     finishedThreads = 0;

  public OConsoleProgressWriter(OOperationsSet operationsSet, OStressTestResults results, int threadsNumber) {
    this.operationsSet = operationsSet;
    this.results = results;
    this.threadsNumber = threadsNumber;
  }

  public void printMessage(String message) {
    System.out.println(message);
  }

  private void updateConsole() {

    final long totalOperations = operationsSet.getTotalOperations();
    final Map<OOperationType, AtomicLong> progress = results.getProgress();

    System.out.print(String.format("\rStress test in progress %d%% [Creates: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]",
        ((int) (100 * (progress.get(OOperationType.CREATE).get() + progress.get(OOperationType.READ).get() + progress
            .get(OOperationType.UPDATE).get() + progress.get(OOperationType.DELETE).get()) / (double) totalOperations)),
        ((int) (100 * (progress.get(OOperationType.CREATE).get() / (double) operationsSet.getNumber(OOperationType.CREATE)))),
        ((int) (100 * (progress.get(OOperationType.READ).get() / (double) operationsSet.getNumber(OOperationType.READ)))),
        ((int) (100 * (progress.get(OOperationType.UPDATE).get() / (double) operationsSet.getNumber(OOperationType.UPDATE)))),
        ((int) (100 * (progress.get(OOperationType.DELETE).get() / (double) operationsSet.getNumber(OOperationType.DELETE))))));
  }

  public void stopProgress() {
    isRunning = (++finishedThreads) != threadsNumber;
  }

  @Override public void run() {
    while (isRunning) {
      updateConsole();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
