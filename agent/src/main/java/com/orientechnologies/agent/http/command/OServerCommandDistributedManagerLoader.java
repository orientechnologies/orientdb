package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandDistributedManagerLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;

  public OServerCommandDistributedManagerLoader(OEnterpriseServer enterpriseServer) {
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandDistributedManager.class.getName();
  }

  @Override
  public String getName() {
    return "distributed-manager";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandDistributedManager(enterpriseServer);
  }
}
