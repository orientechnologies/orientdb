package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 07/07/16. */
public class OLocalResultSet implements OResultSet {

  private OResultSet lastFetch = null;
  private final OInternalExecutionPlan executionPlan;
  private boolean finished = false;
  private long totalExecutionTime = 0;
  private long startTime = 0;
  private boolean closed = false;

  public OLocalResultSet(OInternalExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    fetchNext();
  }

  private boolean fetchNext() {
    long begin = System.currentTimeMillis();
    try {
      if (lastFetch == null) {
        startTime = begin;
      }
      lastFetch = executionPlan.fetchNext(100);
      if (!lastFetch.hasNext()) {
        finished = true;
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
    if (finished || closed) {
      return false;
    }
    if (lastFetch.hasNext()) {
      return true;
    } else {
      return fetchNext();
    }
  }

  @Override
  public OResult next() {
    if (finished) {
      throw new IllegalStateException();
    }
    if (!lastFetch.hasNext()) {
      if (!fetchNext()) {
        throw new IllegalStateException();
      }
    }
    return lastFetch.next();
  }

  private void logProfiling() {
    if (executionPlan.getStatement() != null && Orient.instance().getProfiler().isRecording()) {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null) {
        final OSecurityUser user = db.getUser();
        final String userString = user != null ? user.toString() : null;
        Orient.instance()
            .getProfiler()
            .stopChrono(
                "db."
                    + ODatabaseRecordThreadLocal.instance().get().getName()
                    + ".command.sql."
                    + executionPlan.getStatement(),
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
}
