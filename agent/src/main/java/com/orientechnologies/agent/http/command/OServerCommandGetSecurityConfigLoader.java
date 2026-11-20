package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandGetSecurityConfigLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;

  public OServerCommandGetSecurityConfigLoader(OEnterpriseServer enterpriseServer) {
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandGetSecurityConfig.class.getName();
  }

  @Override
  public String getName() {
    return "security-config";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandGetSecurityConfig(this.enterpriseServer.getSecurity());
  }
}
