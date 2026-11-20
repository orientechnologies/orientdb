package com.orientechnologies.agent.services.studio;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class StudioPermissionCommandLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;

  public StudioPermissionCommandLoader(OEnterpriseServer enterpriseServer) {
    super();
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return StudioPermissionCommand.class.getName();
  }

  @Override
  public String getName() {
    return "studio-permission";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new StudioPermissionCommand(enterpriseServer);
  }
}
