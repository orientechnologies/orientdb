package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SaveElementStep extends AbstractExecutionStep {

  private final OIdentifier cluster;

  public SaveElementStep(OCommandContext ctx, OIdentifier cluster) {
    super(ctx);
    this.cluster = cluster;
  }

  public SaveElementStep(OCommandContext ctx) {
    this(ctx, null);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult result = upstream.next();
        if (result.isElement()) {
          if (cluster == null) {
            ctx.getDatabase().save(result.getElement().orElse(null));
          } else {
            ctx.getDatabase().save(result.getElement().orElse(null), cluster.getStringValue());
          }
        }
        return result;
      }

      @Override public void close() {
        upstream.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
}
