package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OUnwind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwindStep extends AbstractExecutionStep {

  private final OUnwind unwind;
  private List<String> unwindFields;

  public UnwindStep(OUnwind unwind, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.unwind = unwind;
    unwindFields =
        unwind.getItems().stream().map(x -> x.getStringValue()).collect(Collectors.toList());
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = getPrev().get().start(ctx);
    return resultSet.flatMap(this::fetchNextResults);
  }

  private OExecutionStream fetchNextResults(OResult res, OCommandContext ctx) {
    return OExecutionStream.resultIterator(unwind(res, unwindFields, ctx).iterator());
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
