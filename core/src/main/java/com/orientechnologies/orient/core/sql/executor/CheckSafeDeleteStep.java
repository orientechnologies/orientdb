package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * Checks if a record can be safely deleted (throws OCommandExecutionException in case). A record
 * cannot be safely deleted if it's a vertex or an edge (it requires additional operations).
 *
 * <p>The result set returned by syncPull() throws an OCommandExecutionException as soon as it finds
 * a record that cannot be safely deleted (eg. a vertex or an edge)
 *
 * <p>This step is used used in DELETE statement to make sure that you are not deleting vertices or
 * edges without passing for an explicit DELETE VERTEX/EDGE
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CheckSafeDeleteStep extends AbstractExecutionStep {

  public CheckSafeDeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    return attachProfile(upstream.map(this::mapResult));
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result.isElement()) {
      OIdentifiable elem = result.getElement().get();
      ORecord record = elem.getRecord();
      if (record instanceof ODocument) {
        ODocument doc = (ODocument) record;
        OClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
        if (clazz != null) {
          if (clazz.getName().equalsIgnoreCase("V") || clazz.isSubClassOf("V")) {
            throw new OCommandExecutionException(
                "Cannot safely delete a vertex, please use DELETE VERTEX or UNSAFE");
          }
          if (clazz.getName().equalsIgnoreCase("E") || clazz.isSubClassOf("E")) {
            throw new OCommandExecutionException(
                "Cannot safely delete an edge, please use DELETE EDGE or UNSAFE");
          }
        }
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK SAFE DELETE");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }
}
