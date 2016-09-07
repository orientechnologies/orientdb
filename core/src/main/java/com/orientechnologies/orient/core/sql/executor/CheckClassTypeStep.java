package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;

/**
 * Created by luigidellaquila on 05/09/16.
 */
public class CheckClassTypeStep extends AbstractExecutionStep {

  private final String targetClass;
  private final String parentClass;

  boolean found = false;

  public CheckClassTypeStep(String targetClass, String parentClass, OCommandContext ctx) {
    super(ctx);
    this.targetClass = targetClass;
    this.parentClass = parentClass;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (found) {
      return new OInternalResultSet();
    }
    if (this.targetClass.equals(this.parentClass)) {
      return new OInternalResultSet();
    }
    ODatabase db = ctx.getDatabase();

    OSchema schema = db.getMetadata().getSchema();
    OClass parentClazz = schema.getClass(this.parentClass);
    if (parentClazz == null) {
      throw new OCommandExecutionException("Class not found: " + this.parentClass);
    }
    OClass targetClazz = schema.getClass(this.targetClass);
    if (targetClazz == null) {
      throw new OCommandExecutionException("Class not found: " + this.targetClass);
    }

    for (OClass sublcass : parentClazz.getAllSubclasses()) {
      if (sublcass.equals(targetClazz)) {
        this.found = true;
        break;
      }
    }
    if (!found) {
      throw new OCommandExecutionException("Class  " + this.targetClass + " is not a subclass of " + this.parentClass);
    }
    return new OInternalResultSet();
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK CLASS HIERARCHY\n");
    result.append("  " + this.parentClass);
    return result.toString();
  }
}
