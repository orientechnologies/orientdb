package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceOneResult;
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

  private OResultSet resultSet = null;
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
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (resultSet == null) {
      getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
      return new OProduceOneResult(
          () -> {
            long begin = profilingEnabled ? System.nanoTime() : 0;
            try {
              OIndex idx =
                  ctx.getDatabase().getMetadata().getIndexManager().getIndex(target.getIndexName());
              Object val =
                  idx.getDefinition().createValue(keyValue.execute(new OResultInternal(), ctx));
              long size = idx.getInternal().getRids(val).distinct().count();
              OResultInternal result = new OResultInternal();
              result.setProperty(alias, size);
              return result;
            } finally {
              count += (System.nanoTime() - begin);
            }
          },
          true);
    }
    return resultSet;
  }

  @Override
  public void reset() {
    this.resultSet = null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE BY KEY: " + target;
  }
}
