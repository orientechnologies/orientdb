package com.orientechnologies.agent.functions;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.server.OClientConnectionStats;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Collectors;

/** Created by Enrico Risa on 23/07/2018. */
public class ListSessionsFunction extends OSQLEnterpriseFunction {

  private OEnterpriseServer server;

  public ListSessionsFunction(OEnterpriseServer server) {
    super("listSessions", 0, 0);

    this.server = server;
  }

  @Override
  public Object exec(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {
    return server.getConnections().stream()
        .filter((c -> sameDatabase(c, iContext)))
        .map(
            (c) -> {
              final ONetworkProtocolData data = c.getData();
              final OClientConnectionStats stats = c.getStats();

              final DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              final String lastCommandOn;
              final String connectedOn;
              synchronized (dateTimeFormat) {
                lastCommandOn = dateTimeFormat.format(new Date(stats.lastCommandReceived));
                connectedOn = dateTimeFormat.format(new Date(c.getSince()));
              }
              String lastDatabase;
              String lastUser;
              if (stats.lastDatabase != null && stats.lastUser != null) {
                lastDatabase = stats.lastDatabase;
                lastUser = stats.lastUser;
              } else {
                lastDatabase = data.lastDatabase;
                lastUser = data.lastUser;
              }
              OResultInternal internal = new OResultInternal();

              internal.setProperty("connectionId", c.getId());
              internal.setProperty(
                  "remoteAddress",
                  c.getProtocol().getChannel() != null
                      ? c.getProtocol().getChannel().toString()
                      : "Disconnected");
              internal.setProperty("db", lastDatabase != null ? lastDatabase : "-");
              internal.setProperty("user", lastUser != null ? lastUser : "-");
              internal.setProperty("totalRequests", stats.totalRequests);
              internal.setProperty("commandInfo", data.commandInfo);
              internal.setProperty("commandDetail", data.commandDetail);
              internal.setProperty("lastCommandOn", lastCommandOn);
              internal.setProperty("lastCommandInfo", stats.lastCommandInfo);
              internal.setProperty("lastCommandDetail", stats.lastCommandDetail);
              internal.setProperty("lastExecutionTime", stats.lastCommandExecutionTime);
              internal.setProperty("totalWorkingTime", stats.totalCommandExecutionTime);
              internal.setProperty("activeQueries", stats.activeQueries);
              internal.setProperty("connectedOn", connectedOn);
              internal.setProperty("protocol", c.getProtocol().getType());
              internal.setProperty("sessionId", data.sessionId);
              internal.setProperty("clientId", data.clientId);

              final StringBuilder driver = new StringBuilder(128);
              if (data.driverName != null) {
                driver.append(data.driverName);
                driver.append(" v");
                driver.append(data.driverVersion);
                driver.append(" Protocol v");
                driver.append(data.protocolVersion);
              }
              internal.setProperty("driver", driver.toString());
              return internal;
            })
        .collect(Collectors.toList());
  }

  @Override
  public ORule.ResourceGeneric genericPermission() {
    return ORule.ResourceGeneric.DATABASE;
  }

  @Override
  public String specificPermission() {
    return "listSessions";
  }

  @Override
  public String getSyntax() {
    return "listSessions()";
  }
}
