package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceOneResult;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;

/**
 * Returns the number of records contained in an index
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromIndexStep extends AbstractExecutionStep {
  private final OIndexIdentifier target;
  private final String alias;

  private long count = 0;
  private OResultSet resultSet = null;

  /**
   * @param targetIndex the index name as it is parsed by the SQL parsed
   * @param alias the name of the property returned in the result-set
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromIndexStep(
      OIndexIdentifier targetIndex, String alias, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetIndex;
    this.alias = alias;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (resultSet == null) {
      getPrev().ifPresent(x -> x.syncPull(ctx));
      resultSet = new OProduceOneResult(() -> produce(ctx), true);
    }
    return resultSet;
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
      OIndexInternal idx =
          database
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(database, target.getIndexName())
              .getInternal();
      long size = idx.size();
      OResultInternal result = new OResultInternal();
      result.setProperty(alias, size);
      return result;
    } finally {
      count += (System.nanoTime() - begin);
    }
  }

  @Override
  public void reset() {
    this.resultSet = null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE: " + target;
  }
}
