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

import com.orientechnologies.orient.stresstest.OMode;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.util.OConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The output of the entire test.
 *
 * @author Andrea Iacono
 */
public class OStressTestResults {

  final private String     OPERATION_RESULT                 = "\nAverage time for %,d %s: %.2f secs [%dth percentile] - Throughput: %,d/s.";
  final private List<Long> operationsExecutorCreatesResults = new ArrayList<Long>();
  final private List<Long> operationsExecutorReadsResults   = new ArrayList<Long>();
  final private List<Long> operationsExecutorUpdatesResults = new ArrayList<Long>();
  final private List<Long> operationsExecutorDeletesResults = new ArrayList<Long>();

  // public fields are used for mapping to JSON for output results
  public final OOperationsSet operationsSet;
  public final OMode          mode;
  public       int            threadsNumber;
  public       int            iterationsNumber;
  public       long           totalTime;

  public double averageCreatesTime;
  public double averageReadsTime;
  public double averageUpdatesTime;
  public double averageDeletesTime;

  public int createsPercentile;
  public int readsPercentile;
  public int updatesPercentile;
  public int deletesPercentile;

  public long createsThroughput;
  public long readsThroughput;
  public long updatesThroughput;
  public long deletesThroughput;

  public OStressTestResults(OOperationsSet operationsSet, OMode mode, int threadsNumber, int iterationsNumber) {
    this.operationsSet = operationsSet;
    this.mode = mode;
    this.threadsNumber = threadsNumber;
    this.iterationsNumber = iterationsNumber;
  }

  public void addTotalExecutionTime(long totalTime) {
    this.totalTime = totalTime;
  }

  /**
   * Adds the output of a single executor. It will be called N times
   * (where N is the number of threads defined for the test by the number of iterations)
   *
   * @param operationsExecutorResults
   */
  public void addThreadResults(OOperationsExecutorResults operationsExecutorResults) {
    operationsExecutorCreatesResults.add(operationsExecutorResults.getCreatesTime());
    operationsExecutorReadsResults.add(operationsExecutorResults.getReadsTime());
    operationsExecutorUpdatesResults.add(operationsExecutorResults.getUpdatesTime());
    operationsExecutorDeletesResults.add(operationsExecutorResults.getDeletesTime());
  }

  @Override
  public String toString() {

    StringBuilder results = new StringBuilder();
    results.append("OrientDB Stress Test v").append(OConstants.VERSION).append("\n").append(getParameters()).append("\n");

    if (totalTime != 0) {
      results.append("\nTotal execution time: ").append(String.format("%.2f", totalTime / 1000f)).append(" seconds.");
    }

    long totalCreatesTime = 1;
    long totalReadsTime = 1;
    long totalUpdatesTime = 1;
    long totalDeletesTime = 1;

    for (int j = 0; j < operationsExecutorCreatesResults.size(); j++) {
      totalCreatesTime += operationsExecutorCreatesResults.get(j);
      totalReadsTime += operationsExecutorReadsResults.get(j);
      totalUpdatesTime += operationsExecutorUpdatesResults.get(j);
      totalDeletesTime += operationsExecutorDeletesResults.get(j);
    }

    averageCreatesTime = computeAverage(totalCreatesTime);
    averageReadsTime = computeAverage(totalReadsTime);
    averageUpdatesTime = computeAverage(totalUpdatesTime);
    averageDeletesTime = computeAverage(totalDeletesTime);

    createsPercentile = getPercentile(averageCreatesTime, operationsExecutorCreatesResults);
    readsPercentile = getPercentile(averageReadsTime, operationsExecutorReadsResults);
    updatesPercentile = getPercentile(averageUpdatesTime, operationsExecutorUpdatesResults);
    deletesPercentile = getPercentile(averageDeletesTime, operationsExecutorDeletesResults);

    createsThroughput = (int) ((operationsSet.getNumberOfCreates() / (float) averageCreatesTime));
    readsThroughput = (int) ((operationsSet.getNumberOfReads() / (float) averageReadsTime));
    updatesThroughput = (int) ((operationsSet.getNumberOfUpdates() / (float) averageUpdatesTime));
    deletesThroughput = (int) ((operationsSet.getNumberOfDeletes() / (float) averageDeletesTime));

    results.append(
        String.format(OPERATION_RESULT, operationsSet.getNumberOfCreates(), "Creates", averageCreatesTime, createsPercentile, createsThroughput))
        .append(String.format(OPERATION_RESULT, operationsSet.getNumberOfReads(), "Reads", averageReadsTime, readsPercentile, readsThroughput))
        .append(String.format(OPERATION_RESULT, operationsSet.getNumberOfUpdates(), "Updates", averageUpdatesTime, updatesPercentile, updatesThroughput))
        .append(String.format(OPERATION_RESULT, operationsSet.getNumberOfDeletes(), "Deletes", averageDeletesTime, deletesPercentile, deletesThroughput))
        .append("\n");

    return results.toString();
  }

  /**
   * computes the percentile of the average time compared to all the times
   *
   * @param averageTime
   * @param operationsExecutorResults
   * @return
   */
  private int getPercentile(double averageTime, List<Long> operationsExecutorResults) {
    int average = (int) (averageTime * 1000);
    Collections.sort(operationsExecutorResults);
    int j = 0;
    for (; j < operationsExecutorResults.size(); j++) {
      Long value = operationsExecutorResults.get(j);
      if (value > average) {
        break;
      }
    }
    return (int) (100 * (j / (float) operationsExecutorResults.size()));
  }

  private double computeAverage(long totalOperationTime) {
    return (totalOperationTime / (double) (threadsNumber * iterationsNumber)) / 1000;
  }

  private StringBuilder getParameters() {
    return new StringBuilder("Mode: ").append(mode.toString()).append(", Threads: ").append(threadsNumber).append(", Iterations: ")
        .append(iterationsNumber).append(", Operations: ").append(operationsSet.toString());
  }
}
