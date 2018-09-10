package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

public interface ODistributedExecutable {

  void executeDistributed(OCoordinatedExecutor executor);
}
