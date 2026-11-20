package com.orientechnologies.agent.services.security;

import com.orientechnologies.agent.http.command.OServerCommandAuditing;
import com.orientechnologies.agent.http.command.OServerCommandAuditingLoader;
import com.orientechnologies.agent.http.command.OServerCommandGetSecurityConfig;
import com.orientechnologies.agent.http.command.OServerCommandGetSecurityConfigLoader;
import com.orientechnologies.agent.http.command.OServerCommandPostSecurityReload;
import com.orientechnologies.agent.http.command.OServerCommandPostSecurityReloadLoader;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

/** Created by Enrico Risa on 18/09/2018. */
public class OSecurityService implements OEnterpriseService {

  private OEnterpriseServer server;

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
  }

  @Override
  public void start() {
    server.registerStatelessCommand(new OServerCommandAuditingLoader(server));
    server.registerStatelessCommand(new OServerCommandGetSecurityConfigLoader(server));
    server.registerStatelessCommand(new OServerCommandPostSecurityReloadLoader(server));
  }

  @Override
  public void stop() {
    server.unregisterStatelessCommand(OServerCommandAuditing.class);
    server.unregisterStatelessCommand(OServerCommandGetSecurityConfig.class);
    server.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
  }
}
