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
package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.Set;

/**
 * Abstract class to extend to build Custom SQL Functions. Extend it and register it with: <code>
 * OSQLParser.getInstance().registerStatelessFunction()</code> or <code>
 * OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSQLFunctionAbstract implements OSQLFunction {
  protected String name;
  protected int minParams;
  protected int maxParams;

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
  public void config(final Object[] iConfiguredParameters) {}

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
  public void setResult(final Object iResult) {}

  @Override
  public boolean shouldMergeDistributedResult() {
    return false;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    throw new IllegalStateException("By default SQL function execution result cannot be merged");
  }

  protected boolean returnDistributedResult() {
    return OScenarioThreadLocal.INSTANCE.isRunModeDistributed();
  }

  protected String getDistributedStorageId() {
    return ODatabaseRecordThreadLocal.instance().get().getStorageId();
  }

  /**
   * Attempt to extract a single item from object if it's a multi value {@link OMultiValue} If
   * source is a multi value
   *
   * @param source a value to attempt extract single value from it
   * @return If source is not a multi value, it will return source as is. If it is, it will return
   *     the single element in it. If source is a multi value with more than 1 element null is
   *     returned, indicating an error
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  protected Object getSingleItem(Object source) {
    if (OMultiValue.isMultiValue(source)) {
      if (OMultiValue.getSize(source) > 1) {
        return null;
      }
      source = OMultiValue.getFirstValue(source);
      if (source instanceof OResult && ((OResult) source).isElement()) {
        source = ((OResult) source).getElement().get();
      }
    }
    return source;
  }

  /**
   * Attempts to identify the source as a map-like object with single property and return it.
   *
   * @param source The object to check
   * @param requireSingleProperty True if the method should return null when source doesn't have a
   *     single property. Otherwise, the object will be returned.
   * @return If source is a map-like object with single property, that property will be returned If
   *     source is a map-like object with multiple properties and requireSingleProperty is true,
   *     null is returned indicating an error If source is not a map-like object, it is returned
   */
  protected Object getSingleProperty(Object source, boolean requireSingleProperty) {
    if (source instanceof OResult) {
      final OResult result = (OResult) source;
      // TODO we might want to add .size() and iterator with .next() to OResult. The current
      // implementation is
      // quite heavy compared to the result we actually want (the single first property).
      final Set<String> propertyNames = result.getPropertyNames();
      if (propertyNames.size() != 1) {
        return requireSingleProperty ? null : source;
      }
      return result.getProperty(propertyNames.iterator().next());
    }
    return source;
  }
}
