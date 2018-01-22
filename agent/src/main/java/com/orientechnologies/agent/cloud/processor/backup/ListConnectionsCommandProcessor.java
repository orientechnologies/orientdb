package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.tasks.ListConnectionsTask;
import com.orientechnologies.agent.cloud.processor.tasks.ListConnectionsTaskResponse;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.ServerInfo;

import java.io.StringWriter;

public class ListConnectionsCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerInfo request = (ServerInfo) command.getPayload();

    if (!agent.isDistributed()) {
      String asJson = getConnectionsAsJson(agent.server);
      response.setPayload(asJson);
    } else {
      OperationResponseFromNode res = agent.getNodesManager().send(request.getName(), new ListConnectionsTask());
      NodeResponse nodeResponse = res.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
        ListConnectionsTaskResponse taskResponse = (ListConnectionsTaskResponse) ok.getPayload();
        response.setPayload(taskResponse.getConnections());
      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %d", request.getName()), "");
      }

    }
    return response;
  }

  public static String getConnectionsAsJson(OServer server) {
    final StringWriter jsonBuffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(jsonBuffer);
    try {

      json.beginObject();
      OServerInfo.getConnections(server, json, null);

      json.endObject();

      return jsonBuffer.toString();
    } catch (Exception e) {
    }
    return "";
  }

}

