/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command;

import java.util.Locale;
import java.util.Map;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * Abstract implementation of Executor Command interface.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public abstract class OCommandExecutorAbstract extends OBaseParser implements OCommandExecutor {
  protected OProgressListener   progressListener;
  protected int                 limit = -1;
  protected Map<Object, Object> parameters;
  protected OCommandContext     context;

  public OCommandExecutorAbstract init(final OCommandRequestText iRequest) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    parserText = iRequest.getText().trim();
    parserTextUpperCase = parserText.toUpperCase(Locale.ENGLISH);
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [text=" + parserText + "]";
  }

  public OProgressListener getProgressListener() {
    return progressListener;
  }

  public <RET extends OCommandExecutor> RET setProgressListener(final OProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  public int getLimit() {
    return limit;
  }

  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    this.limit = iLimit;
    return (RET) this;
  }

  public Map<Object, Object> getParameters() {
    return parameters;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  public OCommandContext getContext() {
    if (context == null)
      context = new OBasicCommandContext();
    return context;
  }

  public void setContext(final OCommandContext iContext) {
    context = iContext;
  }

  public static ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
