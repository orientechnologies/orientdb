package com.orientechnologies.orient.distributed.context.coordination.topology;

public class ONodeInfo {
  private long lastReceivedNotification;

  public ONodeInfo() {
    lastReceivedNotification = System.nanoTime();
  }

  public void ping() {
    lastReceivedNotification = System.nanoTime();
  }

  public boolean awayMoreThan(long timeInMills) {
    return lastReceivedNotification + timeInMills * 1000000 > System.nanoTime();
  }
}
