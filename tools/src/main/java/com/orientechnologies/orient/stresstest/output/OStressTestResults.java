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
import java.util.List;

/**
 * The output of the entire test.
 *
 * @author Andrea Iacono
 */
public class OStressTestResults {

    private StringBuffer results;
    private long totalTime;
    private List<OOperationsExecutorResults> operationsExecutorResultses;
    private OOperationsSet operationsSet;
    private OMode mode;
    private int threadsNumber;
    private int iterationsNumber;

    public OStressTestResults(OOperationsSet operationsSet, OMode mode, int threadsNumber, int iterationsNumber) {
        this.operationsSet = operationsSet;
        this.mode = mode;
        this.threadsNumber = threadsNumber;
        this.iterationsNumber = iterationsNumber;
        results = new StringBuffer();
        operationsExecutorResultses = new ArrayList<>();
    }

    public void addTotalExecutionTime(long totalTime) {
        this.totalTime = totalTime;
    }

    /**
     * Adds the output of a single executor. It will be called N times
     * (where N is the number of threads defined for the test)
     *
     * @param operationsExecutorResults
     */
    public void addThreadResults(OOperationsExecutorResults operationsExecutorResults) {
        operationsExecutorResultses.add(operationsExecutorResults);
    }

    @Override
    public String toString() {

        results.append("OrientDB Stress Test v")
                .append(OConstants.VERSION)
                .append("\n")
                .append(getParameters())
                .append("\n");

        if (totalTime != 0) {
            results.append("\nTotal execution time: ")
                    .append(String.format("%.2f", totalTime / (float) 1_000))
                    .append(" seconds.");
        }

        long totalCreatesTime = 1;
        long totalReadsTime = 1;
        long totalUpdatesTime = 1;
        long totalDeletesTime = 1;

        for (OOperationsExecutorResults result : operationsExecutorResultses) {
            totalCreatesTime += result.getCreatesTime();
            totalReadsTime += result.getReadsTime();
            totalUpdatesTime += result.getUpdatesTime();
            totalDeletesTime += result.getDeletesTime();
        }

        final double averageCreatesTime = (totalCreatesTime / (double) (threadsNumber * iterationsNumber)) / 1000;
        final double averageReadsTime = (totalReadsTime / (double) (threadsNumber * iterationsNumber)) / 1000;
        final double averageUpdatesTime = (totalUpdatesTime / (double) (threadsNumber * iterationsNumber)) / 1000;
        final double averageDeletesTime = (totalDeletesTime / (double) (threadsNumber * iterationsNumber)) / 1000;

        final long createsPerSecond = (int) ((operationsSet.getNumberOfCreates() / (float) averageCreatesTime));
        final long readsPerSecond = (int) ((operationsSet.getNumberOfReads() / (float) averageReadsTime));
        final long updatesPerSecond = (int) ((operationsSet.getNumberOfUpdates() / (float) averageUpdatesTime));
        final long deletesPerSecond = (int) ((operationsSet.getNumberOfDeletes() / (float) averageDeletesTime));

        results.append(String.format("\nAverage time for %,d Creates: %.2f secs (%,d/s).", operationsSet.getNumberOfCreates(), averageCreatesTime, createsPerSecond))
                .append(String.format("\nAverage time for %,d Reads: %.2f secs (%,d/s).", operationsSet.getNumberOfReads(), averageReadsTime, readsPerSecond))
                .append(String.format("\nAverage time for %,d Updates: %.2f secs (%,d/s).", operationsSet.getNumberOfUpdates(), averageUpdatesTime, updatesPerSecond))
                .append(String.format("\nAverage time for %,d Deletes: %.2f secs (%,d/s).", operationsSet.getNumberOfDeletes(), averageDeletesTime, deletesPerSecond))
                .append("\n");

        return results.toString();
    }

    private StringBuilder getParameters() {
        return new StringBuilder("OMode: ")
                .append(mode.toString())
                .append(", Threads: ")
                .append(threadsNumber)
                .append(", Iterations: ")
                .append(iterationsNumber)
                .append(", Operations: ")
                .append(operationsSet.toString());
    }
}
