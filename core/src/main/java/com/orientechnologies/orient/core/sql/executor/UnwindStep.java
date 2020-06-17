package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OUnwind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwindStep extends AbstractExecutionStep {

  private final OUnwind unwind;
  private List<String> unwindFields;

  private OResultSet lastResult = null;
  private Iterator<OResult> nextSubsequence = null;
  private OResult nextElement = null;

  public UnwindStep(OUnwind unwind, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.unwind = unwind;
    unwindFields =
        unwind.getItems().stream().map(x -> x.getStringValue()).collect(Collectors.toList());
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    return new OResultSet() {
      private long localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextElement == null) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextElement == null) {
          throw new IllegalStateException();
        }

        OResult result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(ctx, nRecords);
        return result;
      }

      @Override
      public void close() {}

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNext(OCommandContext ctx, int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().get().syncPull(ctx, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      OResult nextAggregateItem = lastResult.next();
      nextSubsequence = unwind(nextAggregateItem, unwindFields, ctx).iterator();

    } while (true);
  }

  private Collection<OResult> unwind(
      final OResult doc, final List<String> unwindFields, final OCommandContext iContext) {
    final List<OResult> result = new ArrayList<>();

    if (unwindFields.size() == 0) {
      result.add(doc);
    } else {
      String firstField = unwindFields.get(0);
      final List<String> nextFields = unwindFields.subList(1, unwindFields.size());

      Object fieldValue = doc.getProperty(firstField);
      if (fieldValue == null || fieldValue instanceof ODocument) {
        result.addAll(unwind(doc, nextFields, iContext));
        return result;
      }

      if (!(fieldValue instanceof Iterable) && !fieldValue.getClass().isArray()) {
        result.addAll(unwind(doc, nextFields, iContext));
        return result;
      }

      Iterator iterator;
      if (fieldValue.getClass().isArray()) {
        iterator = OMultiValue.getMultiValueIterator(fieldValue);
      } else {
        iterator = ((Iterable) fieldValue).iterator();
      }
      if (!iterator.hasNext()) {
        OResultInternal unwindedDoc = new OResultInternal();
        copy(doc, unwindedDoc);

        unwindedDoc.setProperty(firstField, null);
        result.addAll(unwind(unwindedDoc, nextFields, iContext));
      } else {
        do {
          Object o = iterator.next();
          OResultInternal unwindedDoc = new OResultInternal();
          copy(doc, unwindedDoc);
          unwindedDoc.setProperty(firstField, o);
          result.addAll(unwind(unwindedDoc, nextFields, iContext));
        } while (iterator.hasNext());
      }
    }

    return result;
  }

  private void copy(OResult from, OResultInternal to) {
    for (String prop : from.getPropertyNames()) {
      to.setProperty(prop, from.getProperty(prop));
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ " + unwind;
  }
}
