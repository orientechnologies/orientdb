package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryListenerV2;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 15/06/17. */
public class LiveQueryListenerImpl implements OLiveQueryListenerV2 {

  public static final String BEFORE_METADATA_KEY = "$$before$$";
  private final OLiveQueryResultListener clientListener;
  private ODatabaseDocument execDb;

  private final OSelectStatement statement;
  private String className;
  private List<ORecordId> rids;

  private final Map<Object, Object> params;

  private int token;
  private static final Random random = new Random();

  public LiveQueryListenerImpl(
      OLiveQueryResultListener clientListener, String query, ODatabaseDocument db, Object[] iArgs) {
    this(clientListener, query, db, toPositionalParams(iArgs));
  }

  public LiveQueryListenerImpl(
      OLiveQueryResultListener clientListener,
      String query,
      ODatabaseDocument db,
      Map<Object, Object> iArgs) {
    this.clientListener = clientListener;
    this.params = iArgs;

    if (query.trim().toLowerCase().startsWith("live ")) {
      query = query.trim().substring(5);
    }
    OStatement stm = OSQLEngine.parse(query, (ODatabaseDocumentInternal) db);
    if (!(stm instanceof OSelectStatement)) {
      throw new OCommandExecutionException(
          "Only SELECT statement can be used as a live query: " + query);
    }
    this.statement = (OSelectStatement) stm;
    validateStatement(statement);
    if (statement.getTarget().getItem().getIdentifier() != null) {
      this.className = statement.getTarget().getItem().getIdentifier().getStringValue();
      if (!((ODatabaseDocumentInternal) db)
          .getMetadata()
          .getImmutableSchemaSnapshot()
          .existsClass(className)) {
        throw new OCommandExecutionException(
            "Class " + className + " not found in the schema: " + query);
      }
    } else if (statement.getTarget().getItem().getRids() != null) {
      this.rids =
          statement.getTarget().getItem().getRids().stream()
              .map(x -> x.toRecordId(new OResultInternal(), new OBasicCommandContext()))
              .collect(Collectors.toList());
    }
    execInSeparateDatabase(
        new OCallable() {
          @Override
          public Object call(Object iArgument) {
            return execDb = ((ODatabaseDocumentInternal) db).copy();
          }
        });

    synchronized (random) {
      token = random.nextInt(); // TODO do something better ;-)!
    }
    OLiveQueryHookV2.subscribe(token, this, (ODatabaseInternal) db);

    OCommandContext ctx = new OBasicCommandContext();
    if (iArgs != null)
    // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
    {
      for (Map.Entry<Object, Object> arg : iArgs.entrySet()) {
        ctx.setVariable(arg.getKey().toString(), arg.getValue());
      }
    }
  }

  private void validateStatement(OSelectStatement statement) {
    if (statement.getProjection() != null) {
      if (statement.getProjection().getItems().stream().anyMatch(x -> x.isAggregate())) {
        throw new OCommandExecutionException(
            "Aggregate Projections cannot be used in live query " + statement);
      }
    }
    if (statement.getTarget().getItem().getIdentifier() == null
        && statement.getTarget().getItem().getRids() == null) {
      throw new OCommandExecutionException(
          "Live queries can only be executed against a Class or on RIDs" + statement);
    }
    if (statement.getOrderBy() != null) {
      throw new OCommandExecutionException("Live queries do not support ORDER BY " + statement);
    }
    if (statement.getGroupBy() != null) {
      throw new OCommandExecutionException("Live queries do not support GROUP BY " + statement);
    }
    if (statement.getSkip() != null || statement.getLimit() != null) {
      throw new OCommandExecutionException("Live queries do not support SKIP/LIMIT " + statement);
    }
  }

  public int getToken() {
    return token;
  }

  @Override
  public void onLiveResults(List<OLiveQueryHookV2.OLiveQueryOp> iRecords) {
    execDb.activateOnCurrentThread();

    for (OLiveQueryHookV2.OLiveQueryOp iRecord : iRecords) {
      OResultInternal record;
      if (iRecord.type == ORecordOperation.CREATED || iRecord.type == ORecordOperation.UPDATED) {
        record = copy(iRecord.after);
        if (iRecord.type == ORecordOperation.UPDATED) {
          OResultInternal before = copy(iRecord.before);
          record.setMetadata(BEFORE_METADATA_KEY, before);
        }
      } else {
        record = copy(iRecord.before);
        record.setMetadata(BEFORE_METADATA_KEY, record);
      }

      if (filter(record)) {
        switch (iRecord.type) {
          case ORecordOperation.DELETED:
            record.setMetadata(BEFORE_METADATA_KEY, null);
            clientListener.onDelete(execDb, applyProjections(record));
            break;
          case ORecordOperation.UPDATED:
            OResult before =
                applyProjections((OResultInternal) record.getMetadata(BEFORE_METADATA_KEY));
            record.setMetadata(BEFORE_METADATA_KEY, null);
            clientListener.onUpdate(execDb, before, applyProjections(record));
            break;
          case ORecordOperation.CREATED:
            clientListener.onCreate(execDb, applyProjections(record));
            break;
        }
      }
    }
    if (clientListener instanceof OLiveQueryBatchResultListener) {
      ((OLiveQueryBatchResultListener) clientListener).onBatchEnd(execDb);
    }
  }

  private OResultInternal applyProjections(OResultInternal record) {
    if (statement.getProjection() != null) {
      OResultInternal result =
          (OResultInternal)
              statement.getProjection().calculateSingle(new OBasicCommandContext(), record);
      return result;
    }
    return record;
  }

  private boolean filter(OResult record) {
    // filter by class
    if (className != null) {
      Object filterClass = record.getProperty("@class");
      String recordClassName = String.valueOf(filterClass);
      if (filterClass == null) {
        return false;
      } else if (!(className.equalsIgnoreCase(recordClassName))) {
        OClass recordClass =
            ((ODatabaseDocumentInternal) this.execDb)
                .getMetadata()
                .getImmutableSchemaSnapshot()
                .getClass(recordClassName);
        if (recordClass == null) {
          return false;
        }
        if (!recordClass.getName().equalsIgnoreCase(className)
            && !recordClass.isSubClassOf(className)) {
          return false;
        }
      }
    }
    if (rids != null && rids.size() > 0) {
      boolean found = false;
      for (ORecordId rid : rids) {
        if (rid.equals(record.getIdentity().orElse(null))) {
          found = true;
          break;
        }
        if (rid.equals(record.getProperty("@rid"))) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    // filter conditions
    OWhereClause where = statement.getWhereClause();
    if (where == null) {
      return true;
    }
    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setInputParameters(params);
    return where.matchesFilters(record, ctx);
  }

  private OResultInternal copy(OResult item) {
    if (item == null) {
      return null;
    }
    OResultInternal result = new OResultInternal();

    for (String prop : item.getPropertyNames()) {
      result.setProperty(prop, item.getProperty(prop));
    }
    return result;
  }

  private static Map<Object, Object> toPositionalParams(Object[] iArgs) {
    Map<Object, Object> result = new HashMap<>();
    for (int i = 0; i < iArgs.length; i++) {
      result.put(i, iArgs[i]);
    }
    return result;
  }

  @Override
  public void onLiveResultEnd() {
    clientListener.onEnd(execDb);
  }

  protected void execInSeparateDatabase(final OCallable iCallback) {
    final ODatabaseDocumentInternal prevDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      iCallback.call(null);
    } finally {
      if (prevDb != null) {
        ODatabaseRecordThreadLocal.instance().set(prevDb);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  public OSelectStatement getStatement() {
    return statement;
  }
}
