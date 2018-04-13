package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.tasks;

public final class ForceTask implements Task {
  @Override
  public TaskType getType() {
    return TaskType.FORCE;
  }
}
