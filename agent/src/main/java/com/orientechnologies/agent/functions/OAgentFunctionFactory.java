package com.orientechnologies.agent.functions;

import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 23/07/2018.
 */
public class OAgentFunctionFactory implements OEnterpriseService {

  private OEnterpriseServer server;

  private List<OSQLFunction> eeFunctions = new ArrayList<>();

  public OAgentFunctionFactory() {

  }

  @Override
  public void init(OEnterpriseServer server) {
    this.server = server;
    this.eeFunctions.add(new ListQueriesFunction(server));
    this.eeFunctions.add(new KillQueryFunction(server));
  }

  @Override
  public void start() {

    this.eeFunctions.forEach(f -> this.server.registerFunction(f));

  }

  @Override
  public void stop() {
    this.eeFunctions.forEach(f -> this.server.unregisterFunction(f.getName()));
  }
}
