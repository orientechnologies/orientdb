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
import com.orientechnologies.orient.stresstest.operations.OOperationType;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.util.OConstants;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The output of the entire test.
 *
 * @author Andrea Iacono
 */
public class OStressTestResults {

  final private StringBuilder testParameters;
  final private Map<OOperationType, AtomicLong> progress                 = new HashMap<OOperationType, AtomicLong>();
  final private Map<OOperationType, List<Long>> operationsPartialResults = new HashMap<OOperationType, List<Long>>();

  // public fields are used for automatic mapping to JSON in output results
  public long totalTime;

  final public OOperationsSet operationsSet;
  final public OMode          mode;
  final public int            threadsNumber;
  final public int            operationsPerTransaction;

  final public Map<OOperationType, Long>       totalTimesInMillisecs     = new HashMap<OOperationType, Long>();
  final public Map<OOperationType, Double>     actualTotalTimesInSeconds = new HashMap<OOperationType, Double>();
  final public Map<OOperationType, Long>       throughputs               = new HashMap<OOperationType, Long>();
  final public Map<OOperationType, Integer>    percentiles               = new HashMap<OOperationType, Integer>();


  public OStressTestResults(OOperationsSet operationsSet, OMode mode, int threadsNumber, int operationsPerTransaction) {
    this.operationsSet = operationsSet;
    this.mode = mode;
    this.threadsNumber = threadsNumber;
    this.operationsPerTransaction = operationsPerTransaction;
    testParameters = getTestParameters(mode, threadsNumber, operationsPerTransaction, operationsSet);

    // init maps
    for (OOperationType type : OOperationType.values()) {
      operationsPartialResults.put(type, new ArrayList<Long>());
      progress.put(type, new AtomicLong(0));
      totalTimesInMillisecs.put(type, 0L);
    }
  }

  public void addTotalExecutionTime(long totalTime) {
    this.totalTime = totalTime;
  }

  /**
   * Adds the output of a single executor. It will be called N times
   * (where N is the number of threads defined for the test)
   */
  public void addThreadResults(Map<OOperationType, Long> partialTimes) {
    for (OOperationType type : OOperationType.values()) {
      totalTimesInMillisecs.put(type, totalTimesInMillisecs.get(type) + partialTimes.get(type));
    }
  }

  public void addPartialResult(OOperationType type, long value) {
    operationsPartialResults.get(type).add(value);
  }

  @Override public String toString() {

    StringBuilder results = new StringBuilder("OrientDB Stress Test v")
        .append(OConstants.VERSION)
        .append("\n")
        .append(testParameters)
        .append("\n\nTotal execution time: ")
        .append(String.format("%.2f", totalTime / 1000d))
        .append(" seconds.");

    for (OOperationType type : OOperationType.values()) {

      // divides the total time of operations by 1000 (for transforming
      // the timings from millisecs to secs) and by the thread number (e.g. if
      // total time of 4 threads is 20 secs, the time actually spent is
      // 5 secs since the threads had run in parallel)
      actualTotalTimesInSeconds.put(type, totalTimesInMillisecs.get(type) / (1000d * threadsNumber));

      percentiles.put(type, getPercentile(totalTimesInMillisecs.get(type), operationsPartialResults.get(type)));
      throughputs.put(type, getThroughput(operationsSet.getNumber(type), actualTotalTimesInSeconds.get(type)));

      // appends values to results string
      results.append(String.format(
          "\nTime for %,d %s: %.2f secs [%dth percentile] - Throughput: %,d/s.",
          operationsSet.getNumber(type),
          type.getName(),
          actualTotalTimesInSeconds.get(type),
          percentiles.get(type),
          throughputs.get(type))
      );
    }

    return results.toString();
  }

  /**
   * computes the percentile of the average time compared to all the partial times
   */
  private int getPercentile(double totalTime, List<Long> partialResults) {
    int average = (int) (totalTime / partialResults.size());
    Collections.sort(partialResults);
    int j = 0;
    for (; j < partialResults.size(); j++) {
      Long value = partialResults.get(j);
      if (value > average) {
        break;
      }
    }
    return (int) (100 * (j / (float) partialResults.size()));
  }

  private long getThroughput(long numberOfOperations, double totalOperationTime) {
    return (long) (numberOfOperations / totalOperationTime);
  }

  private StringBuilder getTestParameters(OMode mode, int threadsNumber, int operationsPerTransaction,
      OOperationsSet operationsSet) {
    return new StringBuilder("Mode: ")
        .append(mode.toString())
        .append(", Threads: ")
        .append(threadsNumber)
        .append(", Operations: ")
        .append(operationsSet.toString())
        .append(", OperationsPerTx: ")
        .append(operationsPerTransaction == 0 ? "No Tx" : operationsPerTransaction);
  }

  public void setTestProgress(OOperationType operationType, int numberOfOperations) {
    progress.get(operationType).addAndGet(numberOfOperations);
  }

  Map<OOperationType, AtomicLong> getProgress() {
    return progress;
  }
}
