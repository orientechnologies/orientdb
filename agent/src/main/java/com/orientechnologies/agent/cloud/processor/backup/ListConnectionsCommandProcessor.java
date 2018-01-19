package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
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

    }

    return response;
  }

  public static String getConnectionsAsJson(OServer server) {
    try {
      final StringWriter jsonBuffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(jsonBuffer);
      json.beginObject();
      OServerInfo.getConnections(server, json, null);

      json.endObject();

      return json.toString();
    } catch (Exception e) {

    }
    return "";
  }

}

