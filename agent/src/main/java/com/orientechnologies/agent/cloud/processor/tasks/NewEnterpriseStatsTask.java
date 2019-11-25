package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Optional;

/**
 * Created by Enrico Risa on 16/01/2018.
 */
public class NewEnterpriseStatsTask implements NodeOperation {
  @Override
  public NodeOperationResponse execute(final OServer iServer, final ODistributedServerManager iManager) {
    final OEnterpriseAgent agent = iServer.getPluginByClass(OEnterpriseAgent.class);
    final Optional<OrientDBMetricsService> metrics = agent.getServiceByClass(OrientDBMetricsService.class);

    if (metrics.isPresent()) {
      return new EnterpriseStatsResponse(metrics.get().toJson());
    }
    return new EnterpriseStatsResponse("{}");
  }

  @Override
  public void write(DataOutput out) {

  }

  @Override
  public void read(DataInput in) {

  }

  @Override
  public int getMessageId() {
    return 2;
  }
}
