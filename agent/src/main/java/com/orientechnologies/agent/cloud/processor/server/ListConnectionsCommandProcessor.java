package com.orientechnologies.agent.cloud.processor.server;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.backup.AbstractBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.tasks.ListConnectionsTask;
import com.orientechnologies.agent.cloud.processor.tasks.ListConnectionsTaskResponse;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionStats;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orientdb.cloud.protocol.*;

import java.util.List;
import java.util.stream.Collectors;

public class ListConnectionsCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerInfo request = (ServerInfo) command.getPayload();

    if (!agent.isDistributed()) {
      response.setPayload(getConnections(agent.server));
    } else {
      OperationResponseFromNode res = agent.getNodesManager().send(request.getName(), new ListConnectionsTask());
      NodeResponse nodeResponse = res.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
        ListConnectionsTaskResponse taskResponse = (ListConnectionsTaskResponse) ok.getPayload();
        response.setPayload(taskResponse.getPayload());
      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %d", request.getName()), "");
      }

    }
    return response;
  }

  public static ServerConnections getConnections(OServer server) {

    ServerConnections connections = new ServerConnections();
    final List<OClientConnection> conns = server.getClientConnectionManager().getConnections();

    List<ServerConnection> serverConnections = conns.stream().map((c) -> mapConnection(c)).collect(Collectors.toList());

    connections.setConnections(serverConnections);
    return connections;
  }

  private static ServerConnection mapConnection(OClientConnection c) {
    final ONetworkProtocolData data = c.getData();
    final OClientConnectionStats stats = c.getStats();
    ServerConnection conn = new ServerConnection();

    String lastDatabase;
    String lastUser;
    conn.setConnectionId(c.getId());
    conn.setSince(c.getSince());
    if (stats.lastDatabase != null && stats.lastUser != null) {
      lastDatabase = stats.lastDatabase;
      lastUser = stats.lastUser;
    } else {
      lastDatabase = data.lastDatabase;
      lastUser = data.lastUser;
    }
    conn.setRemoteAddress(c.getProtocol().getChannel() != null ? c.getProtocol().getChannel().toString() : "Disconnected");
    conn.setLastCommandOn(stats.lastCommandReceived);
    conn.setDatabase(lastDatabase);
    conn.setUser(lastUser);
    conn.setTotalRequest(stats.totalRequests);
    conn.setCommandInfo(data.commandInfo);
    conn.setCommandDetail(data.commandDetail);
    conn.setLastCommandInfo(stats.lastCommandInfo);
    conn.setLastCommandDetail(stats.lastCommandDetail);
    conn.setLastCommandExecutionTime(stats.lastCommandExecutionTime);
    conn.setTotalCommandExecutionTime(stats.totalCommandExecutionTime);
    conn.setProtocolType(c.getProtocol().getType());
    conn.setSessionId(data.sessionId);
    conn.setClientId(data.clientId);
    conn.setDriverName(data.driverName);
    conn.setDriverVersion(data.driverVersion);
    conn.setProtocolVersion(data.protocolVersion);

    return conn;
  }

}

