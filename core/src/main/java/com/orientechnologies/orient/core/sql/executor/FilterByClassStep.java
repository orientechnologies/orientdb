package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Optional;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClassStep extends AbstractExecutionStep {

  private OIdentifier identifier;
  private String className;

  public FilterByClassStep(OIdentifier identifier, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
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
    if (result.isElement()) {
      Optional<OClass> clazz = result.getElement().get().getSchemaType();
      if (clazz.isPresent() && clazz.get().isSubClassOf(className)) {
        return result;
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(depth, indent));
    result.append("+ FILTER ITEMS BY CLASS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append(" \n");
    result.append(OExecutionStepInternal.getIndent(depth, indent));
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
  public OExecutionStep copy(OCommandContext ctx) {
    return new FilterByClassStep(this.identifier.copy(), ctx, this.profilingEnabled);
  }
}
