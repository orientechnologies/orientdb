package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import java.util.Collections;

/**
 * Returns the number of records contained in an index
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromIndexStep extends AbstractExecutionStep {
  private final OIndexIdentifier target;
  private final String alias;

  /**
   * @param targetIndex the index name as it is parsed by the SQL parsed
   * @param alias the name of the property returned in the result-set
   */
  public CountFromIndexStep(OIndexIdentifier targetIndex, String alias) {
    super();
    this.target = targetIndex;
    this.alias = alias;
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
              ((OBasicCommandContext) context)
                  .updateProfilerIndex(
                      Collections.singletonList(
                          new OIndexStreamStat(
                              target.getIndexName(), idx.getDefinition().getParamCount(), 0)));
            });
  }

  private OResult produce(OCommandContext ctx) {
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
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    return spaces + "+ CALCULATE INDEX SIZE: " + target;
  }
}
