package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Objects;

/** Does nothing, just set the termination flag.
 *
 */
public class OTerminationExecutionStream implements OExecutionStream {

  private OExecutionStream from;

  public OTerminationExecutionStream(OExecutionStream from) {
    Objects.requireNonNull(from);
    this.from = from;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return from.hasNext(ctx);
  }

  @Override
  public OResult next(OCommandContext ctx) {
    return from.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    from.close(ctx);
  }

  @Override
  public boolean isFullInMemory(OCommandContext ctx) {
    return from.isFullInMemory(ctx);
  }

  @Override
  public boolean isTermination(OCommandContext ctx) {
    return true;
  }
}
