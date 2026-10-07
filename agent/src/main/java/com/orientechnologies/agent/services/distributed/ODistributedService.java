package com.orientechnologies.agent.services.distributed;

import com.orientechnologies.agent.functions.OAgentProfilerService;
import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

public class ODistributedService implements OEnterpriseService {

  private OEnterpriseServer server;

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
    this.server.registerStatelessCommand(new OServerCommandDistributedManager(server));
  }

  @Override
  public void start() {

    server
        .getServiceByClass(OAgentProfilerService.class)
        .ifPresent(
            (e) -> {
              if (this.server.getDatabases() instanceof OServerAware sa) {
                ODistributedServerManager distributedManager = sa.getDistributedManager();
                if (distributedManager != null) {
                  OEnterpriseProfiler profiler = e.getProfiler();
                  if (profiler != null) {
                    distributedManager.registerLifecycleListener(profiler);
                  }
                }
              }
            });
  }

  @Override
  public void stop() {
    this.server.unregisterStatelessCommand(OServerCommandDistributedManager.class);
  }
}
