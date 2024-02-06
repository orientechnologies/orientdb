package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * This step is used just as a gate check for classes (eg. for CREATE VERTEX to make sure that the
 * passed class is a vertex class).
 *
 * <p>It accepts two values: a target class and a parent class. If the two classes are the same or
 * if the parent class is indeed a parent class of the target class, then the syncPool() returns an
 * empty result set, otherwise it throws an OCommandExecutionException
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckClassTypeStep extends AbstractExecutionStep {

  private final String targetClass;
  private final String parentClass;

  /**
   * @param targetClass a class to be checked
   * @param parentClass a class that is supposed to be the same or a parent class of the target
   *     class
   * @param ctx execuiton context
   * @param profilingEnabled true to collect execution stats
   */
  public CheckClassTypeStep(
      String targetClass, String parentClass, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.parentClass = parentClass;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext context) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(context).close(ctx));
    if (this.targetClass.equals(this.parentClass)) {
      return OExecutionStream.empty();
    }
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();

    OSchema schema = db.getMetadata().getImmutableSchemaSnapshot();
    OClass parentClazz = schema.getClass(this.parentClass);
    if (parentClazz == null) {
      throw new OCommandExecutionException("Class not found: " + this.parentClass);
    }
    OClass targetClazz = schema.getClass(this.targetClass);
    if (targetClazz == null) {
      throw new OCommandExecutionException("Class not found: " + this.targetClass);
    }

    boolean found = false;
    if (parentClazz.equals(targetClazz)) {
      found = true;
    } else {
      for (OClass sublcass : parentClazz.getAllSubclasses()) {
        if (sublcass.equals(targetClazz)) {
          found = true;
          break;
        }
      }
    }
    if (!found) {
      throw new OCommandExecutionException(
          "Class  " + this.targetClass + " is not a subclass of " + this.parentClass);
    }
    return OExecutionStream.empty();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK CLASS HIERARCHY");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append("  " + this.parentClass);
    return result.toString();
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new CheckClassTypeStep(targetClass, parentClass, ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
