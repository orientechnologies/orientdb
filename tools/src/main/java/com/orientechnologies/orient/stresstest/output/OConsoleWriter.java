package com.orientechnologies.orient.stresstest.output;

import com.orientechnologies.orient.stresstest.operations.OOperationsSet;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Takes care of updating the console with a completion percentage
 * while the stress test is working
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
