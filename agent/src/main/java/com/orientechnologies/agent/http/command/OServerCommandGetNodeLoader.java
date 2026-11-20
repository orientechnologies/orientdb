package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandGetNodeLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;

  public OServerCommandGetNodeLoader(OEnterpriseServer enterpriseServer) {
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandGetNode.class.getName();
  }

  @Override
  public String getName() {
    return "get-node";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandGetNode(enterpriseServer);
  }
}
