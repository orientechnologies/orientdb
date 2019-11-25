package com.orientechnologies.agent.services.studio;

import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

/**
 * Created by Enrico Risa on 29/08/2018.
 */
public class StudioService implements OEnterpriseService {

  private OEnterpriseServer server;

  @Override
  public void init(final OEnterpriseServer server) {
    this.server = server;
  }

  @Override
  public void start() {
    server.registerStatelessCommand(new StudioPermissionCommand(server));
  }

  @Override
  public void stop() {
    server.unregisterStatelessCommand(StudioPermissionCommand.class);
  }
}
