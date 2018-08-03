package com.orientechnologies.agent.functions;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 23/07/2018.
 */
public class ListQueriesFunction extends OSQLFunctionAbstract {

  private OEnterpriseServer server;

  public ListQueriesFunction(OEnterpriseServer server) {
    super("listQueries", 0, 0);

    this.server = server;
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    return server.getConnections().stream().filter((c) -> c.getDatabase() != null)
        .flatMap((c) -> c.getDatabase().getActiveQueries().entrySet().stream().map((k) -> {
          OResultInternal internal = new OResultInternal();
          internal.setProperty("queryId", k.getKey());
          OResultSet resultSet = k.getValue();
          Optional<OExecutionPlan> plan = resultSet.getExecutionPlan();
          String query = plan.map((p -> {
            String q = "";
            if (p instanceof OInternalExecutionPlan) {
              String stm = ((OInternalExecutionPlan) p).getStatement();
              if (stm != null) {
                q = stm;
              }
            }
            return q;
          })).orElse("");
          internal.setProperty("query", query);
          if (resultSet instanceof OLocalResultSetLifecycleDecorator) {
            OResultSet oResultSet = ((OLocalResultSetLifecycleDecorator) resultSet).getInternal();
            if (oResultSet instanceof OLocalResultSet) {
              internal.setProperty("elapsedTimeMillis", ((OLocalResultSet) oResultSet).getTotalExecutionTime());
            }
          }
          return internal;
        })).collect(Collectors.toList());

  }

  @Override
  public String getSyntax() {
    return "listQueries()";
  }
}
