package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class SaveElementStep extends AbstractExecutionStep {

  private final OIdentifier cluster;

  public SaveElementStep(OIdentifier cluster) {
    super();
    this.cluster = cluster;
  }

  public SaveElementStep() {
    this(null);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result.isElement()) {
      if (cluster == null) {
        ctx.getDatabase().save(result.getElement().orElse(null));
      } else {
        ctx.getDatabase().save(result.getElement().orElse(null), cluster.getStringValue());
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SAVE RECORD");
    if (cluster != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on cluster " + cluster);
    }
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new SaveElementStep(cluster == null ? null : cluster.copy());
  }
}
