/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORestrictedAccessHook;
import com.orientechnologies.orient.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import java.util.Map;
import java.util.Random;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCommandExecutorSQLLiveSelect extends OCommandExecutorSQLSelect
    implements OLiveQueryListener {
  public static final String KEYWORD_LIVE_SELECT = "LIVE SELECT";
  private ODatabaseDocument execDb;
  private int token;
  private static final Random random = new Random();

  public OCommandExecutorSQLLiveSelect() {}

  public Object execute(final Map<Object, Object> iArgs) {
    try {
      final ODatabaseDocumentInternal db = getDatabase();
      execInSeparateDatabase(
          new OCallable() {
            @Override
            public Object call(Object iArgument) {
              return execDb = db.copy();
            }
          });

      synchronized (random) {
        token = random.nextInt(); // TODO do something better ;-)!
      }
      subscribeToLiveQuery(token, db);
      bindDefaultContextVariables();

      if (iArgs != null)
      // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
      {
        for (Map.Entry<Object, Object> arg : iArgs.entrySet()) {
          context.setVariable(arg.getKey().toString(), arg.getValue());
        }
      }

      if (timeoutMs > 0) {
        getContext().beginExecution(timeoutMs, timeoutStrategy);
      }

      ODocument result = new ODocument();
      result.field("token", token); // TODO change this name...?

      ((OLegacyResultSet) getResult()).add(result);
      return getResult();
    } finally {
      if (request != null && request.getResultListener() != null) {
        request.getResultListener().end();
      }
    }
  }

  private void subscribeToLiveQuery(Integer token, ODatabaseInternal db) {
    OLiveQueryHook.subscribe(token, this, db);
  }

  public void onLiveResult(final ORecordOperation iOp) {

    ODatabaseDocumentInternal oldThreadLocal = ODatabaseRecordThreadLocal.instance().getIfDefined();
    execDb.activateOnCurrentThread();

    try {
      final OIdentifiable value = iOp.getRecord();

      if (!matchesTarget(value)) {
        return;
      }
      if (!matchesFilters(value)) {
        return;
      }
      if (!checkSecurity(value)) {
        return;
      }
    } finally {
      if (oldThreadLocal == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(oldThreadLocal);
      }
    }
    final OCommandResultListener listener = request.getResultListener();
    if (listener instanceof OLiveResultListener) {
      execInSeparateDatabase(
          new OCallable() {
            @Override
            public Object call(Object iArgument) {
              execDb.activateOnCurrentThread();
              ((OLiveResultListener) listener).onLiveResult(token, iOp);
              return null;
            }
          });
    }
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

  private boolean checkSecurity(OIdentifiable value) {
    try {
      // TODO check this!
      execDb.checkSecurity(
          ORule.ResourceGeneric.CLASS,
          ORole.PERMISSION_READ,
          ((ODocument) value.getRecord()).getClassName());
    } catch (OSecurityException ignore) {
      return false;
    }
    OSecurityInternal security =
        ((ODatabaseDocumentInternal) execDb).getSharedContext().getSecurity();
    boolean allowedByPolicy = security.canRead((ODatabaseSession) execDb, value.getRecord());
    return allowedByPolicy
        && ORestrictedAccessHook.isAllowed(
            (ODatabaseDocumentInternal) execDb,
            (ODocument) value.getRecord(),
            ORestrictedOperation.ALLOW_READ,
            false);
  }

  private boolean matchesFilters(OIdentifiable value) {
    if (this.compiledFilter == null || this.compiledFilter.getRootCondition() == null) {
      return true;
    }
    if (!(value instanceof ODocument)) {
      value = value.getRecord();
    }
    return !(Boolean.FALSE.equals(
        compiledFilter.evaluate((ODocument) value, (ODocument) value, getContext())));
  }

  private boolean matchesTarget(OIdentifiable value) {
    if (!(value instanceof ODocument)) {
      return false;
    }
    final String className = ((ODocument) value).getClassName();
    if (className == null) {
      return false;
    }
    final OClass docClass = execDb.getMetadata().getSchema().getClass(className);
    if (docClass == null) {
      return false;
    }

    if (this.parsedTarget.getTargetClasses() != null) {
      for (String clazz : parsedTarget.getTargetClasses().keySet()) {
        if (docClass.isSubClassOf(clazz)) {
          return true;
        }
      }
    }
    if (this.parsedTarget.getTargetRecords() != null) {
      for (OIdentifiable r : parsedTarget.getTargetRecords()) {
        if (r.getIdentity().equals(value.getIdentity())) {
          return true;
        }
      }
    }
    if (this.parsedTarget.getTargetClusters() != null) {
      final String clusterName = execDb.getClusterNameById(value.getIdentity().getClusterId());
      if (clusterName != null) {
        for (String cluster : parsedTarget.getTargetClusters().keySet()) {
          if (clusterName.equalsIgnoreCase(cluster)) { // make it case insensitive in 3.0?
            return true;
          }
        }
      }
    }
    return false;
  }

  public void onLiveResultEnd() {
    if (request.getResultListener() instanceof OLiveResultListener) {
      ((OLiveResultListener) request.getResultListener()).onUnsubscribe(token);
    }

    if (execDb != null) {
      ODatabaseDocumentInternal oldThreadDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
      execDb.activateOnCurrentThread();
      execDb.close();
      if (oldThreadDB == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(oldThreadDB);
      }
    }
  }

  @Override
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    final OCommandRequestText requestText = (OCommandRequestText) iRequest;
    final String originalText = requestText.getText();
    final String remainingText = requestText.getText().trim().substring(5).trim();
    requestText.setText(remainingText);
    try {
      return super.parse(iRequest);
    } finally {
      requestText.setText(originalText);
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }
}
