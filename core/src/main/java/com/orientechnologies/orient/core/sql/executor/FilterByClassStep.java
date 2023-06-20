package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OFilterResultSet;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Optional;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClassStep extends AbstractExecutionStep {

  private OIdentifier identifier;
  private long cost;

  public FilterByClassStep(OIdentifier identifier, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.identifier = identifier;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    OExecutionStream resultSet = prev.get().syncPull(ctx);
    return new OFilterResultSet(resultSet, this::filterMap);
  }

  private OResult filterMap(OResult result) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (result.isElement()) {
        Optional<OClass> clazz = result.getElement().get().getSchemaType();
        if (clazz.isPresent() && clazz.get().isSubClassOf(identifier.getStringValue())) {
          return result;
        }
      }
      return null;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
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
  public long getCost() {
    return cost;
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
