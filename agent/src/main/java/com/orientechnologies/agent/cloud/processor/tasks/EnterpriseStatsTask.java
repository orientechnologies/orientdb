package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Created by Enrico Risa on 16/01/2018.
 */
public class EnterpriseStatsTask implements NodeOperation {
  @Override
  public NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager) {
    return new EnterpriseStatsResponse(Orient.instance().getProfiler().getStatsAsJson());
  }

  @Override
  public void write(DataOutput out) {

  }

  @Override
  public void read(DataInput in) {

  }

  @Override
  public int getMessageId() {
    return 1;
  }

}
