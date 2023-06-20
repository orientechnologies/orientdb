package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceResultSet;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;

/**
 * Returns the number of records contained in an index
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromIndexWithKeyStep extends AbstractExecutionStep {
  private final OIndexIdentifier target;
  private final String alias;
  private final OExpression keyValue;

  private long count = 0;

  /**
   * @param targetIndex the index name as it is parsed by the SQL parsed
   * @param alias the name of the property returned in the result-set
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromIndexWithKeyStep(
      OIndexIdentifier targetIndex,
      OExpression keyValue,
      String alias,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetIndex;
    this.alias = alias;
    this.keyValue = keyValue;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return new OProduceResultSet(() -> produce(ctx)).limit(1);
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      OIndex idx =
          ctx.getDatabase().getMetadata().getIndexManager().getIndex(target.getIndexName());
      Object val = idx.getDefinition().createValue(keyValue.execute(new OResultInternal(), ctx));
      long size = idx.getInternal().getRids(val).distinct().count();
      OResultInternal result = new OResultInternal();
      result.setProperty(alias, size);
      return result;
    } finally {
      count += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE BY KEY: " + target;
  }
}
