package com.orientechnologies.orient.server.distributed.operation;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NodeOperationTask implements ORemoteTask {
  public static final int FACTORYID = 55;
  private NodeOperation task;
  private String        nodeSource;

  public NodeOperationTask(NodeOperation task) {
    this.task = task;
  }

  public NodeOperationTask() {
  }

  @Override
  public boolean hasResponse() {
    return true;
  }

  @Override
  public String getName() {
    return "Node Task";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    return new NodeOperationTaskResponse(task.getMessageId(), task.execute(iServer, iManager));
  }

  @Override
  public int[] getPartitionKey() {
    //This should be the best number for this use case checking the execution implementation
    return new int[-2];
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong();
  }

  @Override
  public long getSynchronousTimeout(int iSynchNodes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTotalTimeout(int iTotalNodes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNodeSource() {
    return this.nodeSource;
  }

  @Override
  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  @Override
  public boolean isIdempotent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return true;
  }

  @Override
  public boolean isUsingDatabase() {
    return false;
  }

  @Override
  public int getFactoryId() {
    return 0;
  }

  @Override
  public void checkIsValid(ODistributedServerManager dManager) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(task.getMessageId());
    task.write(out);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    int messageId = in.readInt();
    task = createOperation(messageId);
    task.read(in);
  }

  private static NodeOperation createOperation(int messageId) {
    return null;
  }

  public static NodeOperationResponse createOperationResponse(int messageId) {
    return null;
  }
}
