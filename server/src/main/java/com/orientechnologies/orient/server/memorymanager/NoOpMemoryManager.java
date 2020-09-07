package com.orientechnologies.orient.server.memorymanager;

public final class NoOpMemoryManager implements MemoryManager {
    @Override
    public void start() {}

    @Override
    public void shutdown() {}

    @Override
    public void checkAndWaitMemoryThreshold() {}
}
