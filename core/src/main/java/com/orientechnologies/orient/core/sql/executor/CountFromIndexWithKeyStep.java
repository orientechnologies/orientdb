package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import java.util.Collections;
import java.util.List;

/**
 * Returns the number of records contained in an index
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromIndexWithKeyStep extends AbstractExecutionStep {
  private final OIndexIdentifier target;
  private final String alias;
  private final OExpression keyValue;

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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return OExecutionStream.produce(this::produce)
        .limit(1)
        .onClose(
            (OCommandContext context) -> {
              final ODatabaseDocumentInternal database =
                  (ODatabaseDocumentInternal) ctx.getDatabase();
              OIndexInternal idx =
                  database
                      .getMetadata()
                      .getIndexManagerInternal()
                      .getIndex(database, target.getIndexName())
                      .getInternal();
              List<OIndexStreamStat> stats =
                  Collections.singletonList(
                      new OIndexStreamStat(
                          target.getIndexName(), idx.getDefinition().getParamCount(), 1));
              ((OBasicCommandContext) context).updateProfilerIndex(stats);
            });
  }

  private OResult produce(OCommandContext ctx) {
    OIndex idx = ctx.getDatabase().getMetadata().getIndexManager().getIndex(target.getIndexName());
    Object val = idx.getDefinition().createValue(keyValue.execute(new OResultInternal(), ctx));
    long size = idx.getInternal().getRids(val).distinct().count();
    OResultInternal result = new OResultInternal();
    result.setProperty(alias, size);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE BY KEY: " + target;
  }
}
