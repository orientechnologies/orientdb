package com.orientechnologies.agent.functions;

import com.orientechnologies.agent.http.command.OServerCommandGetNode;
import com.orientechnologies.agent.http.command.OServerCommandGetProfiler;
import com.orientechnologies.agent.http.command.OServerCommandGetSQLProfiler;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import java.util.ArrayList;
import java.util.List;

/** Created by Enrico Risa on 23/07/2018. */
public class OAgentProfilerService implements OEnterpriseService {

  private OEnterpriseServer server;

  private List<OSQLFunction> eeFunctions = new ArrayList<>();

  OEnterpriseProfiler profiler;

  public OAgentProfilerService() {}

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
    this.eeFunctions.add(new ListQueriesFunction(server));
    this.eeFunctions.add(new KillQueryFunction(server));
    this.eeFunctions.add(new ListSessionsFunction(server));
    this.eeFunctions.add(new KillSessionFunction(server));
    installProfiler();
  }

  @Override
  public void start() {

    this.eeFunctions.forEach(f -> this.server.registerFunction(f));

    this.server.registerStatelessCommand(new OServerCommandGetSQLProfiler(this.server));
    this.server.registerStatelessCommand(new OServerCommandGetProfiler());

    this.server.registerStatelessCommand(new OServerCommandGetNode(this.server));
  }

  protected void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();
    profiler = new OEnterpriseProfiler(60, currentProfiler, server);

    Orient.instance().setProfiler(profiler);
    Orient.instance().getProfiler().startup();
    if (currentProfiler.isRecording()) {
      profiler.startRecording();
    }
    currentProfiler.shutdown();
  }

  private void uninstallProfiler() {
    final OProfiler currentProfiler = Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OProfilerStub((OAbstractProfiler) currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
    profiler = null;
  }

  @Override
  public void stop() {
    this.eeFunctions.forEach(f -> this.server.unregisterFunction(f.getName()));
    this.server.unregisterStatelessCommand(OServerCommandGetSQLProfiler.class);
    this.server.unregisterStatelessCommand(OServerCommandGetProfiler.class);
    this.server.unregisterStatelessCommand(OServerCommandGetNode.class);
    uninstallProfiler();
  }

  public OEnterpriseProfiler getProfiler() {
    return profiler;
  }
}
