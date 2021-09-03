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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports
 * parent/child context to build a tree of contexts. If a variable is not found on current object
 * the search is applied recursively on child contexts.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OBasicCommandContext implements OCommandContext {
  public static final String EXECUTION_BEGUN = "EXECUTION_BEGUN";
  public static final String TIMEOUT_MS = "TIMEOUT_MS";
  public static final String TIMEOUT_STRATEGY = "TIMEOUT_STARTEGY";
  public static final String INVALID_COMPARE_COUNT = "INVALID_COMPARE_COUNT";

  protected ODatabase database;
  protected Object[] args;

  protected boolean recordMetrics = false;
  protected OCommandContext parent;
  protected OCommandContext child;
  protected Map<String, Object> variables;

  protected Map<Object, Object> inputParameters;

  protected Set<String> declaredScriptVariables = new HashSet<>();

  // MANAGES THE TIMEOUT
  private long executionStartedOn;
  private long timeoutMs;
  private com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY
      timeoutStrategy;
  protected AtomicLong resultsProcessed = new AtomicLong(0);
  protected Set<Object> uniqueResult = new HashSet<Object>();

  public OBasicCommandContext() {}

  public Object getVariable(String iName) {
    return getVariable(iName, null);
  }

  public Object getVariable(String iName, final Object iDefault) {
    if (iName == null) return iDefault;

    Object result = null;

    if (iName.startsWith("$")) iName = iName.substring(1);

    int pos = OStringSerializerHelper.getLowerIndexOf(iName, 0, ".", "[");

    String firstPart;
    String lastPart;
    if (pos > -1) {
      firstPart = iName.substring(0, pos);
      if (iName.charAt(pos) == '.') pos++;
      lastPart = iName.substring(pos);
      if (firstPart.equalsIgnoreCase("PARENT") && parent != null) {
        // UP TO THE PARENT
        if (lastPart.startsWith("$")) result = parent.getVariable(lastPart.substring(1));
        else result = ODocumentHelper.getFieldValue(parent, lastPart);

        return result != null ? resolveValue(result) : iDefault;

      } else if (firstPart.equalsIgnoreCase("ROOT")) {
        OCommandContext p = this;
        while (p.getParent() != null) p = p.getParent();

        if (lastPart.startsWith("$")) result = p.getVariable(lastPart.substring(1));
        else result = ODocumentHelper.getFieldValue(p, lastPart, this);

        return result != null ? resolveValue(result) : iDefault;
      }
    } else {
      firstPart = iName;
      lastPart = null;
    }

    if (firstPart.equalsIgnoreCase("CONTEXT")) result = getVariables();
    else if (firstPart.equalsIgnoreCase("PARENT")) result = parent;
    else if (firstPart.equalsIgnoreCase("ROOT")) {
      OCommandContext p = this;
      while (p.getParent() != null) p = p.getParent();
      result = p;
    } else {
      if (variables != null && variables.containsKey(firstPart)) result = variables.get(firstPart);
      else {
        if (child != null) result = child.getVariable(firstPart);
        else result = getVariableFromParentHierarchy(firstPart);
      }
    }

    if (pos > -1) result = ODocumentHelper.getFieldValue(result, lastPart, this);

    return result != null ? resolveValue(result) : iDefault;
  }

  private Object resolveValue(Object value) {
    if (value instanceof ODynamicVariable) {
      value = ((ODynamicVariable) value).resolve(this);
    }
    return value;
  }

  protected Object getVariableFromParentHierarchy(String varName) {
    if (this.variables != null && variables.containsKey(varName)) {
      return variables.get(varName);
    }
    if (parent != null && parent instanceof OBasicCommandContext) {
      return ((OBasicCommandContext) parent).getVariableFromParentHierarchy(varName);
    }
    return null;
  }

  public OCommandContext setDynamicVariable(String iName, final ODynamicVariable iValue) {
    return setVariable(iName, iValue);
  }

  public OCommandContext setVariable(String iName, final Object iValue) {
    if (iName == null) return null;

    if (iName.startsWith("$")) iName = iName.substring(1);

    init();

    int pos = OStringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
    if (pos > -1) {
      Object nested = getVariable(iName.substring(0, pos));
      if (nested != null && nested instanceof OCommandContext)
        ((OCommandContext) nested).setVariable(iName.substring(pos + 1), iValue);
    } else {
      if (variables.containsKey(iName)) {
        variables.put(
            iName, iValue); // this is a local existing variable, so it's bound to current contex
      } else if (parent != null
          && parent instanceof OBasicCommandContext
          && ((OBasicCommandContext) parent).hasVariable(iName)) {
        if ("current".equalsIgnoreCase(iName) || "parent".equalsIgnoreCase(iName)) {
          variables.put(iName, iValue);
        } else {
          parent.setVariable(
              iName,
              iValue); // it is an existing variable in parent context, so it's bound to parent
          // context
        }
      } else {
        variables.put(iName, iValue); // it's a new variable, so it's created in this context
      }
    }
    return this;
  }

  boolean hasVariable(String iName) {
    if (variables != null && variables.containsKey(iName)) {
      return true;
    }
    if (parent != null && parent instanceof OBasicCommandContext) {
      return ((OBasicCommandContext) parent).hasVariable(iName);
    }
    return false;
  }

  @Override
  public OCommandContext incrementVariable(String iName) {
    if (iName != null) {
      if (iName.startsWith("$")) iName = iName.substring(1);

      init();

      int pos = OStringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
      if (pos > -1) {
        Object nested = getVariable(iName.substring(0, pos));
        if (nested != null && nested instanceof OCommandContext)
          ((OCommandContext) nested).incrementVariable(iName.substring(pos + 1));
      } else {
        final Object v = variables.get(iName);
        if (v == null) variables.put(iName, 1);
        else if (v instanceof Number) variables.put(iName, OType.increment((Number) v, 1));
        else
          throw new IllegalArgumentException(
              "Variable '" + iName + "' is not a number, but: " + v.getClass());
      }
    }
    return this;
  }

  public long updateMetric(final String iName, final long iValue) {
    if (!recordMetrics) return -1;

    init();
    Long value = (Long) variables.get(iName);
    if (value == null) value = iValue;
    else value = new Long(value.longValue() + iValue);
    variables.put(iName, value);
    return value.longValue();
  }

  /** Returns a read-only map with all the variables. */
  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    if (child != null) map.putAll(child.getVariables());

    if (variables != null) map.putAll(variables);

    return map;
  }

  /**
   * Set the inherited context avoiding to copy all the values every time.
   *
   * @return
   */
  public OCommandContext setChild(final OCommandContext iContext) {
    if (iContext == null) {
      if (child != null) {
        // REMOVE IT
        child.setParent(null);
        child = null;
      }

    } else if (child != iContext) {
      // ADD IT
      child = iContext;
      iContext.setParent(this);
    }
    return this;
  }

  public OCommandContext getParent() {
    return parent;
  }

  public OCommandContext setParent(final OCommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
      if (parent != null) parent.setChild(this);
    }
    return this;
  }

  public OCommandContext setParentWithoutOverridingChild(final OCommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
    }
    return this;
  }

  @Override
  public String toString() {
    return getVariables().toString();
  }

  public boolean isRecordingMetrics() {
    return recordMetrics;
  }

  public OCommandContext setRecordingMetrics(final boolean recordMetrics) {
    this.recordMetrics = recordMetrics;
    return this;
  }

  @Override
  public void beginExecution(final long iTimeout, final TIMEOUT_STRATEGY iStrategy) {
    if (iTimeout > 0) {
      executionStartedOn = System.currentTimeMillis();
      timeoutMs = iTimeout;
      timeoutStrategy = iStrategy;
    }
  }

  public boolean checkTimeout() {
    if (timeoutMs > 0) {
      if (System.currentTimeMillis() - executionStartedOn > timeoutMs) {
        // TIMEOUT!
        switch (timeoutStrategy) {
          case RETURN:
            return false;
          case EXCEPTION:
            throw new OTimeoutException("Command execution timeout exceed (" + timeoutMs + "ms)");
        }
      }
    } else if (parent != null)
      // CHECK THE TIMER OF PARENT CONTEXT
      return parent.checkTimeout();

    return true;
  }

  @Override
  public OCommandContext copy() {
    final OBasicCommandContext copy = new OBasicCommandContext();
    copy.init();

    if (variables != null && !variables.isEmpty()) copy.variables.putAll(variables);

    copy.recordMetrics = recordMetrics;
    copy.parent = parent;
    copy.child = child;
    return copy;
  }

  @Override
  public void merge(final OCommandContext iContext) {
    // TODO: SOME VALUES NEED TO BE MERGED
  }

  private void init() {
    if (variables == null) variables = new HashMap<String, Object>();
  }

  public Map<Object, Object> getInputParameters() {
    if (inputParameters != null) {
      return inputParameters;
    }

    return parent == null ? null : parent.getInputParameters();
  }

  public void setInputParameters(Map<Object, Object> inputParameters) {
    this.inputParameters = inputParameters;
  }

  /**
   * returns the number of results processed. This is intended to be used with LIMIT in SQL
   * statements
   *
   * @return
   */
  public AtomicLong getResultsProcessed() {
    return resultsProcessed;
  }

  /**
   * adds an item to the unique result set
   *
   * @param o the result item to add
   * @return true if the element is successfully added (it was not present yet), false otherwise (it
   *     was already present)
   */
  public synchronized boolean addToUniqueResult(Object o) {
    Object toAdd = o;
    if (o instanceof ODocument && ((ODocument) o).getIdentity().isNew()) {
      toAdd = new ODocumentEqualityWrapper((ODocument) o);
    }
    return this.uniqueResult.add(toAdd);
  }

  public ODatabase getDatabase() {
    if (database != null) {
      return database;
    }
    if (parent != null) {
      return parent.getDatabase();
    }
    return null;
  }

  public void setDatabase(ODatabase database) {
    this.database = database;
  }

  @Override
  public void declareScriptVariable(String varName) {
    this.declaredScriptVariables.add(varName);
  }

  @Override
  public boolean isScriptVariableDeclared(String varName) {
    if (varName == null || varName.length() == 0) {
      return false;
    }
    String dollarVar = varName;
    if (!dollarVar.startsWith("$")) {
      dollarVar = "$" + varName;
    }
    varName = dollarVar.substring(1);
    if (variables != null && (variables.containsKey(varName) || variables.containsKey(dollarVar))) {
      return true;
    }
    return declaredScriptVariables.contains(varName)
        || declaredScriptVariables.contains(dollarVar)
        || (parent != null && parent.isScriptVariableDeclared(varName));
  }
}
