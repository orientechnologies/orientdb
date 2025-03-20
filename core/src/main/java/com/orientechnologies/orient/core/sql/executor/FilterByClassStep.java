package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Optional;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClassStep extends AbstractExecutionStep {

  private OIdentifier identifier;
  private String className;

  public FilterByClassStep(OIdentifier identifier) {
    super();
    this.identifier = identifier;
    this.className = identifier.getStringValue();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    OExecutionStream resultSet = prev.get().start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    Optional<OElement> element = result.getElement();
    if (element.isPresent()) {
      Optional<OClass> clazz = element.get().getSchemaType();
      if (clazz.isPresent() && clazz.get().isSubClassOf(className)) {
        return result;
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(ctx));
    result.append("+ FILTER ITEMS BY CLASS");
    if (ctx.isProfilingEnabled()) {
      result.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    result.append(" \n");
    result.append(OExecutionStepInternal.getIndent(ctx));
    result.append("  ");
    result.append(identifier.getStringValue());
    return result.toString();
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("identifier", identifier.serialize());

    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      identifier = OIdentifier.deserialize(fromResult.getProperty("identifier"));
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new FilterByClassStep(this.identifier.copy());
  }
}
