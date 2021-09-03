package com.orientechnologies.orient.server.distributed.tx;

public class TxWriterConfig {
  public final String writerId;
  public final int vertexGroupId;
  public final int groupSize;
  public final int txMaxRetries;
  public final int txBatchSize;
  public final int noOfGroups;
  public final boolean createEdgesRandomly;

  public TxWriterConfig(
      String writerId,
      int vertexGroupId,
      int groupSize,
      int txMaxRetries,
      int txBatchSize,
      int noOfGroups,
      boolean createEdgesRandomly) {
    this.writerId = writerId;
    this.vertexGroupId = vertexGroupId;
    this.groupSize = groupSize;
    this.txMaxRetries = txMaxRetries;
    this.txBatchSize = txBatchSize;
    this.noOfGroups = noOfGroups;
    this.createEdgesRandomly = createEdgesRandomly;
  }
}
