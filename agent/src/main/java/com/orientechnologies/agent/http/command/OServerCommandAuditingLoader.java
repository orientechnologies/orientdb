package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandAuditingLoader implements OServerCommandLoader {

  private OEnterpriseServer server;

  public OServerCommandAuditingLoader(OEnterpriseServer server) {
    this.server = server;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandAuditing.class.getName();
  }

  @Override
  public String getName() {
    return "auditing";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandAuditing(this.server);
  }
}
