package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceOneResult;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OInsertBody;
import com.orientechnologies.orient.core.sql.parser.OInsertSetExpression;
import java.util.Iterator;
import java.util.List;

/** Created by luigidellaquila on 20/03/17. */
public class InsertIntoIndexStep extends AbstractExecutionStep {
  private final OIndexIdentifier targetIndex;
  private final OInsertBody body;

  private OResultSet result;

  public InsertIntoIndexStep(
      OIndexIdentifier targetIndex,
      OInsertBody insertBody,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetIndex = targetIndex;
    this.body = insertBody;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (result == null) {
      result = new OProduceOneResult(() -> produce(ctx));
    }
    return result;
  }

  private OResultInternal produce(OCommandContext ctx) {
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, targetIndex.getIndexName());
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + targetIndex);
    }
    List<OInsertSetExpression> setExps = body.getSetExpressions();
    if (body.getContent() != null) {
      throw new OCommandExecutionException("Invalid expression: INSERT INTO INDEX:... CONTENT ...");
    }
    long count;
    if (setExps != null) {
      count = handleSet(setExps, index, ctx);
    } else {
      count = handleKeyValues(body.getIdentifierList(), body.getValueExpressions(), index, ctx);
    }

    OResultInternal result = new OResultInternal();
    result.setProperty("count", count);
    return result;
  }

  private long handleKeyValues(
      List<OIdentifier> identifierList,
      List<List<OExpression>> setExpressions,
      OIndex index,
      OCommandContext ctx) {
    OExpression keyExp = null;
    OExpression valueExp = null;
    if (identifierList == null || setExpressions == null) {
      throw new OCommandExecutionException("Invalid insert expression");
    }
    long count = 0;
    for (List<OExpression> valList : setExpressions) {
      if (identifierList.size() != valList.size()) {
        throw new OCommandExecutionException("Invalid insert expression");
      }
      for (int i = 0; i < identifierList.size(); i++) {
        OIdentifier key = identifierList.get(i);
        if (key.getStringValue().equalsIgnoreCase("key")) {
          keyExp = valList.get(i);
        }
        if (key.getStringValue().equalsIgnoreCase("rid")) {
          valueExp = valList.get(i);
        }
      }
      count += doExecute(index, ctx, keyExp, valueExp);
    }
    if (keyExp == null || valueExp == null) {
      throw new OCommandExecutionException("Invalid insert expression");
    }
    return count;
  }

  private long handleSet(List<OInsertSetExpression> setExps, OIndex index, OCommandContext ctx) {
    OExpression keyExp = null;
    OExpression valueExp = null;
    for (OInsertSetExpression exp : setExps) {
      if (exp.getLeft().getStringValue().equalsIgnoreCase("key")) {
        keyExp = exp.getRight();
      } else if (exp.getLeft().getStringValue().equalsIgnoreCase("rid")) {
        valueExp = exp.getRight();
      } else {
        throw new OCommandExecutionException("Cannot set " + exp + " on index");
      }
    }
    if (keyExp == null || valueExp == null) {
      throw new OCommandExecutionException("Invalid insert expression");
    }
    return doExecute(index, ctx, keyExp, valueExp);
  }

  private long doExecute(
      OIndex index, OCommandContext ctx, OExpression keyExp, OExpression valueExp) {
    long count = 0;
    Object key = keyExp.execute((OResult) null, ctx);
    Object value = valueExp.execute((OResult) null, ctx);
    if (value instanceof OIdentifiable) {
      insertIntoIndex(index, key, (OIdentifiable) value);
      count++;
    } else if (value instanceof OResult && ((OResult) value).isElement()) {
      insertIntoIndex(index, key, ((OResult) value).getElement().get());
      count++;
    } else if (value instanceof OResultSet) {
      ((OResultSet) value).elementStream().forEach(x -> index.put(key, x));
    } else if (OMultiValue.isMultiValue(value)) {
      Iterator iterator = OMultiValue.getMultiValueIterator(value);
      while (iterator.hasNext()) {
        Object item = iterator.next();
        if (item instanceof OIdentifiable) {
          insertIntoIndex(index, key, (OIdentifiable) item);
          count++;
        } else if (item instanceof OResult && ((OResult) item).isElement()) {
          insertIntoIndex(index, key, ((OResult) item).getElement().get());
          count++;
        } else {
          throw new OCommandExecutionException("Cannot insert into index " + item);
        }
      }
    }
    return count;
  }

  private void insertIntoIndex(final OIndex index, final Object key, final OIdentifiable value) {
    index.put(key, value);
  }
}
