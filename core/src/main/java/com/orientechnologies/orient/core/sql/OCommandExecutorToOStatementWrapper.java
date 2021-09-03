/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper for OPrifileStorageStatement command (for compatibility with the old executor
 * architecture, this component should be removed)
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCommandExecutorToOStatementWrapper implements OCommandExecutor {

  protected OSQLAsynchQuery<ODocument> request;
  private OCommandContext context;
  private OProgressListener progressListener;

  protected OStatement statement;

  @SuppressWarnings("unchecked")
  @Override
  public OCommandExecutorToOStatementWrapper parse(OCommandRequest iCommand) {
    final OCommandRequestText textRequest = (OCommandRequestText) iCommand;
    if (iCommand instanceof OSQLAsynchQuery) {
      request = (OSQLAsynchQuery<ODocument>) iCommand;
    } else {
      // BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
      request = new OSQLSynchQuery<ODocument>(textRequest.getText());
      if (textRequest.getResultListener() != null) {
        request.setResultListener(textRequest.getResultListener());
      }
    }
    String queryText = textRequest.getText();
    statement = OStatementCache.get(queryText, getDatabase());
    return this;
  }

  public static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    return statement.execute(request, context, this.progressListener);
  }

  @Override
  public <RET extends OCommandExecutor> RET setProgressListener(
      OProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  @Override
  public <RET extends OCommandExecutor> RET setLimit(int iLimit) {
    return (RET) this;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  @Override
  public Map<Object, Object> getParameters() {
    return null;
  }

  @Override
  public OCommandContext getContext() {
    return this.context;
  }

  @Override
  public void setContext(OCommandContext context) {
    this.context = context;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return Collections.EMPTY_SET;
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_READ;
  }

  @Override
  public boolean involveSchema() {
    return false;
  }

  @Override
  public String getSyntax() {
    return "PROFILE STORAGE [ON | OFF]";
  }

  @Override
  public boolean isLocalExecution() {
    return true;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  @Override
  public long getDistributedTimeout() {
    return 0;
  }

  @Override
  public Object mergeResults(Map<String, Object> results) throws Exception {
    return null;
  }
}
