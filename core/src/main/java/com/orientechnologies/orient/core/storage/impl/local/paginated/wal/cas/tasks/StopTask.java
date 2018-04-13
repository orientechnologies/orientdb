package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.tasks;

public final class StopTask implements Task {
  @Override
  public TaskType getType() {
    return TaskType.STOP;
  }
}
