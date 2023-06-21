package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromDatabaseMetadataStep extends AbstractExecutionStep {

  private long cost = 0;

  public FetchFromDatabaseMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {

      OResultInternal result = new OResultInternal();

      ODatabaseSession db = ctx.getDatabase();
      result.setProperty("name", db.getName());
      result.setProperty("user", db.getUser() == null ? null : db.getUser().getName());
      result.setProperty("type", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.TYPE)));
      result.setProperty("status", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.STATUS)));
      result.setProperty(
          "defaultClusterId", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.DEFAULTCLUSTERID)));
      result.setProperty(
          "dateFormat", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.DATEFORMAT)));
      result.setProperty(
          "dateTimeFormat", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.DATETIMEFORMAT)));
      result.setProperty("timezone", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.TIMEZONE)));
      result.setProperty(
          "localeCountry", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.LOCALECOUNTRY)));
      result.setProperty(
          "localeLanguage", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.LOCALELANGUAGE)));
      result.setProperty("charset", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.CHARSET)));
      result.setProperty(
          "clusterSelection", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.CLUSTERSELECTION)));
      result.setProperty(
          "minimumClusters", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.MINIMUMCLUSTERS)));
      result.setProperty(
          "conflictStrategy", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.CONFLICTSTRATEGY)));
      result.setProperty(
          "validation", String.valueOf(db.get(ODatabaseSession.ATTRIBUTES.VALIDATION)));

      return result;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
