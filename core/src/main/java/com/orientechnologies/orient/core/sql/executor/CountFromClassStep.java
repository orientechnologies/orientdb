package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceResultSet;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * Returns the number of records contained in a class (including subclasses) Executes a count(*) on
 * a class and returns a single record that contains that value (with a specific alias).
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromClassStep extends AbstractExecutionStep {
  private final OIdentifier target;
  private final String alias;
  private long cost = 0;

  /**
   * @param targetClass An identifier containing the name of the class to count
   * @param alias the name of the property returned in the result-set
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromClassStep(
      OIdentifier targetClass, String alias, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetClass;
    this.alias = alias;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return new OProduceResultSet(() -> produce(ctx)).limit(1);
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {

      OImmutableSchema schema =
          ((ODatabaseDocumentInternal) ctx.getDatabase())
              .getMetadata()
              .getImmutableSchemaSnapshot();
      OClass clazz = schema.getClass(target.getStringValue());
      if (clazz == null) {
        clazz = schema.getView(target.getStringValue());
      }
      if (clazz == null) {
        throw new OCommandExecutionException(
            "Class " + target.getStringValue() + " does not exist in the database schema");
      }
      long size = clazz.count();
      OResultInternal result = new OResultInternal();
      result.setProperty(alias, size);
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
    String result = spaces + "+ CALCULATE CLASS SIZE: " + target;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return false; // explicit: in case of active security policies, the COUNT has to be manual
  }
}
