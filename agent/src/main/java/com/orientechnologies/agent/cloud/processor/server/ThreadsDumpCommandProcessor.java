package com.orientechnologies.agent.cloud.processor.server;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.backup.AbstractBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.tasks.ThreadDumpTask;
import com.orientechnologies.agent.cloud.processor.tasks.ThreadDumpTaskResponse;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.ServerInfo;

public class ThreadsDumpCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerInfo request = (ServerInfo) command.getPayload();

    if (!agent.isDistributed()) {
      String asJson = getThreadDump(agent.server);
      response.setPayload(asJson);
    } else {
      OperationResponseFromNode res = agent.getNodesManager().send(request.getName(), new ThreadDumpTask());
      NodeResponse nodeResponse = res.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
        ThreadDumpTaskResponse taskResponse = (ThreadDumpTaskResponse) ok.getPayload();
        response.setPayload(taskResponse.getPayload());
      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %s", request.getName()), "");
      }

    }
    return response;
  }

  public static String getThreadDump(OServer server) {
    return Orient.instance().getProfiler().threadDump();
  }

}

