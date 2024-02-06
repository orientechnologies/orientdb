package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Optional;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws
 * OCommandExecutionException in case it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {
  private final String clazz;

  public CheckRecordTypeStep(OCommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clazz = className;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = prev.get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CHECK RECORD TYPE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (OExecutionStepInternal.getIndent(depth, indent) + "  " + clazz);
    return result;
  }
}
