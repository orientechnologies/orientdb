package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandBackupManagerLoader implements OServerCommandLoader {
  private OBackupService manager;
  private OEnterpriseServer server;

  public OServerCommandBackupManagerLoader(OBackupService manager, OEnterpriseServer server) {
    this.manager = manager;
    this.server = server;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandBackupManager.class.getName();
  }

  @Override
  public String getName() {
    return "backup-manager";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandBackupManager(manager, this.server);
  }
}
