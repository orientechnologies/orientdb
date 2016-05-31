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

import com.orientechnologies.orient.stresstest.operations.OOperationsSet;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Takes care of updating the console with a completion percentage
 * while the stress test is working
 *
 * @author Andrea Iacono
 */
public class OConsoleWriter {

    private final int totalCreates;
    private final int totalReads;
    private final int totalUpdates;
    private final int totalDeletes;

    private AtomicInteger actualCreates = new AtomicInteger(0);
    private AtomicInteger actualReads = new AtomicInteger(0);
    private AtomicInteger actualUpdates = new AtomicInteger(0);
    private AtomicInteger actualDeletes = new AtomicInteger(0);

    public OConsoleWriter(OOperationsSet operationsSet, int threadsNumber, int iterationsNumber) {
        totalCreates = operationsSet.getNumberOfCreates() * threadsNumber * iterationsNumber;
        totalReads = operationsSet.getNumberOfReads() * threadsNumber * iterationsNumber;
        totalUpdates = operationsSet.getNumberOfUpdates() * threadsNumber * iterationsNumber;
        totalDeletes = operationsSet.getNumberOfDeletes() * threadsNumber * iterationsNumber;
    }

    private void updateConsole() {
        System.out.print(String.format(
                "\rStress test in progress %d%% [Creates: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]",
                ((int) (100 * (actualCreates.get() + actualDeletes.get() + actualReads.get() + actualUpdates.get()) / (float) (totalCreates + totalDeletes + totalReads + totalUpdates))),
                ((int) ((actualCreates.get() / (float) totalCreates) * 100)),
                ((int) ((actualReads.get() / (float) totalReads) * 100)),
                ((int) ((actualUpdates.get() / (float) totalUpdates) * 100)),
                ((int) ((actualDeletes.get() / (float) totalDeletes) * 100)),
                actualReads.get(), totalReads)
        );
    }

    public void addCreate() {
        actualCreates.incrementAndGet();
        updateConsole();
    }

    public void addRead() {
        actualReads.incrementAndGet();
        updateConsole();
    }

    public void addUpdate() {
        actualUpdates.incrementAndGet();
        updateConsole();
    }

    public void addDelete() {
        actualDeletes.incrementAndGet();
        updateConsole();
    }
}
