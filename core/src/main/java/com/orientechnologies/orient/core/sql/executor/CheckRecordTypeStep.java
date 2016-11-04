package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;

import java.util.Map;
import java.util.Optional;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws OCommandExecutionException in case
 * it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {
  private final String clazz;

  public CheckRecordTypeStep(OCommandContext ctx, String c) {
    super(ctx);
    this.clazz = c;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OTodoResultSet upstream = prev.get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult result = upstream.next();
        if (!result.isElement()) {
          throw new OCommandExecutionException("record " + result + " is not an instance of " + clazz);
        }
        OElement doc = result.getElement().get();
        if (doc == null) {
          throw new OCommandExecutionException("record " + result + " is not an instance of " + clazz);
        }
        Optional<OClass> schema = doc.getSchemaType();

        if (!schema.isPresent() || !schema.get().isSubClassOf(clazz)) {
          throw new OCommandExecutionException("record " + result + " is not an instance of " + clazz);
        }
        return result;
      }

      @Override public void close() {
        upstream.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
