package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;
import java.util.Optional;

/**
 * Checks if a record can be safely deleted (throws OCommandExecutionException in case).
 * A record cannot be safely deleted if it's a vertex or an edge (it requires additional operations)
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CheckSafeDeleteStep extends AbstractExecutionStep {
  public CheckSafeDeleteStep(OCommandContext ctx) {
    super(ctx);
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
          OIdentifiable elem = result.getElement();
          ORecord record = elem.getRecord();
          if (record instanceof ODocument) {
            ODocument doc = (ODocument) record;
            OClass clazz = doc.getSchemaClass();
            if (clazz != null) {
              if (clazz.getName().equalsIgnoreCase("V") || clazz.isSubClassOf("V")) {
                throw new OCommandExecutionException("Cannot safelly delete a vertex, please use DELETE VERTEX or UNSAFE");
              }
              if (clazz.getName().equalsIgnoreCase("E") || clazz.isSubClassOf("E")) {
                throw new OCommandExecutionException("Cannot safely delete an edge, please use DELETE EDGE or UNSAFE");
              }
            }
          }
        }
        return result;
      }

      @Override public void close() {

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
    result.append("+ CHECK SAFE DELETE");
    return result.toString();
  }

}
