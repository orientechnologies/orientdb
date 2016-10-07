package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Assigns a class to documents coming from upstream
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SetDocumentClassStep extends AbstractExecutionStep {
  private final String targetClass;

  public SetDocumentClassStep(OIdentifier targetClass, OCommandContext ctx) {
    super(ctx);
    this.targetClass = targetClass.getStringValue();
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OTodoResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult result = upstream.next();
        if (result.isElement()) {
          OIdentifiable element = result.getElement().getRecord();
          if (element instanceof ODocument) {
            ODocument doc = (ODocument) element;
            doc.setClassName(targetClass);
            if (!(result instanceof OResultInternal)) {
              result = new OUpdatableResult(doc);
            } else {
              ((OResultInternal) result).setElement(doc);
            }
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

      @Override public Map<String, Object> getQueryStats() {
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
    result.append("+ SET CLASS\n");
    result.append(spaces);
    result.append("  ");
    result.append(this.targetClass);
    return result.toString();
  }
}
