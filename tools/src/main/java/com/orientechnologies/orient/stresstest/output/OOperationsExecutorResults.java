package com.orientechnologies.orient.stresstest.output;

/**
 * This class contains the times of execution of the operations for an executor (a single thread).
 */
public class OOperationsExecutorResults {

    private long createsTime;
    private long insertsTime;
    private long updatesTime;
    private long deletesTime;

    public OOperationsExecutorResults(long createsTime, long insertsTime, long updatesTime, long deletesTime) {
        this.createsTime = createsTime;
        this.insertsTime = insertsTime;
        this.updatesTime = updatesTime;
        this.deletesTime = deletesTime;
    }

    public long getCreatesTime() {
        return createsTime;
    }

    public long getReadsTime() {
        return insertsTime;
    }

    public long getUpdatesTime() {
        return updatesTime;
    }

    public long getDeletesTime() {
        return deletesTime;
    }
}
