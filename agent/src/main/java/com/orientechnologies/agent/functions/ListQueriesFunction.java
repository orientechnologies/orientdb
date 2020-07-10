package com.orientechnologies.agent.functions;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.Optional;

/** Created by Enrico Risa on 23/07/2018. */
public class ListQueriesFunction extends OSQLEnterpriseFunction {

  private OEnterpriseServer server;

  public ListQueriesFunction(OEnterpriseServer server) {
    super("listQueries", 0, 0);

    this.server = server;
  }

  @Override
  public Object exec(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {

    return server.listQueries(Optional.of((c) -> sameDatabase(c, iContext)));
  }

  @Override
  public ORule.ResourceGeneric genericPermission() {
    return ORule.ResourceGeneric.DATABASE;
  }

  @Override
  public String specificPermission() {
    return "listQueries";
  }

  @Override
  public String getSyntax() {
    return "listQueries()";
  }
}
