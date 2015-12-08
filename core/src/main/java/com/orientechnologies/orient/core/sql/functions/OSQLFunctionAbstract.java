/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;

import java.util.List;

/**
 * Abstract class to extend to build Custom SQL Functions. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionAbstract implements OSQLFunction {
  protected String name;
  protected int    minParams;
  protected int    maxParams;

  public OSQLFunctionAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    this.name = iName;
    this.minParams = iMinParams;
    this.maxParams = iMaxParams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getMinParams() {
    return minParams;
  }

  @Override
  public int getMaxParams() {
    return maxParams;
  }

  @Override
  public String toString() {
    return name + "()";
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public boolean filterResult() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void setResult(final Object iResult) {
  }

  @Override
  public boolean shouldMergeDistributedResult() {
    return false;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    throw new IllegalStateException("By default SQL function execution result cannot be merged");
  }

  protected boolean returnDistributedResult() {
    return OScenarioThreadLocal.INSTANCE.getRunMode() == OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED;
  }

  protected String getDistributedStorageId() {
    return ((OAutoshardedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()).getStorageId();
  }
}
