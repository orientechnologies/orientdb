package com.orientechnologies.agent.http.command;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandGetSQLProfilerLoader implements OServerCommandLoader {

  public OEnterpriseServer enterpriseServer;

  public OServerCommandGetSQLProfilerLoader(OEnterpriseServer enterpriseServer) {
    this.enterpriseServer = enterpriseServer;
  }

  @Override
  public String getCommandClass() {
    return OServerCommandGetSQLProfiler.class.getName();
  }

  @Override
  public String getName() {
    return "enterprise-sql-profiler";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandGetSQLProfiler(enterpriseServer);
  }
}
