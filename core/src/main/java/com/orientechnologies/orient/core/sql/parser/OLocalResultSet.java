package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSetInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 07/07/16. */
public class OLocalResultSet implements OResultSetInternal {

  private OExecutionStream stream = null;
  private final OInternalExecutionPlan executionPlan;
  private final OCommandContext ctx;
  private long totalExecutionTime = 0;
  private long startTime = 0;
  private boolean closed = false;

  public OLocalResultSet(OInternalExecutionPlan executionPlan, OCommandContext ctx) {
    this.executionPlan = executionPlan;
    this.ctx = ctx;
    start();
  }

  private boolean start() {
    long begin = System.currentTimeMillis();
    try {
      if (stream == null) {
        startTime = begin;
      }
      stream = executionPlan.start(ctx);
      if (!stream.hasNext(ctx)) {
        logProfiling();
        return false;
      }
      return true;
    } finally {
      totalExecutionTime += (System.currentTimeMillis() - begin);
    }
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    } else {
      boolean next = stream.hasNext(ctx);
      if (!next) {
        logProfiling();
      }
      return next;
    }
  }

  @Override
  public OResult next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    return stream.next(ctx);
  }

  private void logProfiling() {
    if (executionPlan.getStatement() != null && Orient.instance().getProfiler().isRecording()) {
      final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
      if (db != null) {
        final OSecurityUser user = db.getUser();
        final String userString = user != null ? user.toString() : null;
        Orient.instance()
            .getProfiler()
            .stopChrono(
                "db." + db.getName() + ".command.sql." + executionPlan.getStatement(),
                "Command executed against the database",
                System.currentTimeMillis() - totalExecutionTime,
                "db.*.command.*",
                null,
                userString);
      }
    }
  }

  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public void close() {
    if (!closed) {
      stream.close(ctx);
      executionPlan.close();
      closed = true;
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>(); // TODO
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public boolean isExplain() {
    return false;
  }
}
