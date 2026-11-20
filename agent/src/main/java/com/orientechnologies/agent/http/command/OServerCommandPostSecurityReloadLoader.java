package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandPostSecurityReloadLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;

  public OServerCommandPostSecurityReloadLoader(OEnterpriseServer enterpriseServer) {
    super();
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandPostSecurityReload.class.getName();
  }

  @Override
  public String getName() {
    return "security-reload";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandPostSecurityReload(this.enterpriseServer.getSecurity());
  }
}
