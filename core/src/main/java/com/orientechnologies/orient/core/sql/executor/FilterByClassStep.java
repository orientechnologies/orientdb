package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 01/03/17.
 */
public class FilterByClassStep extends AbstractExecutionStep {
  private OIdentifier identifier;

  OResultSet prevResult = null;

  public FilterByClassStep(OIdentifier identifier, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.identifier = identifier;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OResultSet() {
      public boolean finished = false;

      OResult nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(ctx, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(ctx, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          if (nextItem.isElement()) {
            Optional<OClass> clazz = nextItem.getElement().get().getSchemaType();
            if (clazz.isPresent() && clazz.get().isSubClassOf(identifier.getStringValue())) {
              break;
            }
          }
          nextItem = null;
        }
      }

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        if (nextItem != null) {
          return true;
        }

        return false;
      }

      @Override
      public OResult next() {
        if (fetched >= nRecords || finished) {
          throw new IllegalStateException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new IllegalStateException();
        }
        OResult result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterByClassStep.this.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ FILTER ITEMS BY CLASS \n" + OExecutionStepInternal
        .getIndent(depth, indent) + "  " + identifier.getStringValue();
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
      identifier = new OIdentifier(-1);
      identifier.deserialize(fromResult.getProperty("identifier"));
    } catch (Exception e) {
      throw new OCommandExecutionException("");
    }
  }
}