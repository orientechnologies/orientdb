package com.orientechnologies.agent.functions;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OClientConnection;

/** Created by Enrico Risa on 23/07/2018. */
public class KillQueryFunction extends OSQLEnterpriseFunction {

  private OEnterpriseServer server;

  public KillQueryFunction(OEnterpriseServer server) {
    super("killQuery", 1, 1);

    this.server = server;
  }

  @Override
  public Object exec(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {
    String queryId = iParams[0].toString();

    OResultInternal internal = new OResultInternal();
    String message = String.format("Query %s not found", queryId);
    for (OClientConnection connection : server.getConnections()) {
      if (connection.getDatabase() != null
          && connection
              .getDatabase()
              .getName()
              .equalsIgnoreCase(iContext.getDatabase().getName())) {
        OResultSet resultSet = connection.getDatabase().getActiveQuery(queryId);
        if (resultSet != null) {
          resultSet.close();
          message = String.format("Query %s killed", queryId);
        }
      }
    }
    internal.setProperty("message", message);
    return internal;
  }

  @Override
  public ORule.ResourceGeneric genericPermission() {
    return ORule.ResourceGeneric.DATABASE;
  }

  @Override
  public String specificPermission() {
    return "killQuery";
  }

  @Override
  public String getSyntax() {
    return "killQuery(<queryId>)";
  }
}
