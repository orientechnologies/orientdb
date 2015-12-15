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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports parent/child context to build a tree
 * of contexts. If a variable is not found on current object the search is applied recursively on child contexts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OBasicCommandContext implements OCommandContext {
  public static final String                                                         EXECUTION_BEGUN       = "EXECUTION_BEGUN";
  public static final String                                                         TIMEOUT_MS            = "TIMEOUT_MS";
  public static final String                                                         TIMEOUT_STRATEGY      = "TIMEOUT_STARTEGY";
  public static final String                                                         INVALID_COMPARE_COUNT = "INVALID_COMPARE_COUNT";

  protected boolean                                                                  recordMetrics         = false;
  protected OCommandContext                                                          parent;
  protected OCommandContext                                                          child;
  protected Map<String, Object>                                                      variables;

  // MANAGES THE TIMEOUT
  private long                                                                       executionStartedOn;
  private long                                                                       timeoutMs;
  private com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY timeoutStrategy;

  public OBasicCommandContext() {
  }

  public Object getVariable(String iName) {
    return getVariable(iName, null);
  }

  public Object getVariable(String iName, final Object iDefault) {
    if (iName == null)
      return iDefault;

    Object result = null;

    if (iName.startsWith("$"))
      iName = iName.substring(1);

    int pos = OStringSerializerHelper.getLowerIndexOf(iName, 0, ".", "[");

    String firstPart;
    String lastPart;
    if (pos > -1) {
      firstPart = iName.substring(0, pos);
      if (iName.charAt(pos) == '.')
        pos++;
      lastPart = iName.substring(pos);
      if (firstPart.equalsIgnoreCase("PARENT") && parent != null) {
        // UP TO THE PARENT
        if (lastPart.startsWith("$"))
          result = parent.getVariable(lastPart.substring(1));
        else
          result = ODocumentHelper.getFieldValue(parent, lastPart);

        return result != null ? result : iDefault;

      } else if (firstPart.equalsIgnoreCase("ROOT")) {
        OCommandContext p = this;
        while (p.getParent() != null)
          p = p.getParent();

        if (lastPart.startsWith("$"))
          result = p.getVariable(lastPart.substring(1));
        else
          result = ODocumentHelper.getFieldValue(p, lastPart, this);

        return result != null ? result : iDefault;
      }
    } else {
      firstPart = iName;
      lastPart = null;
    }

    if (firstPart.equalsIgnoreCase("CONTEXT"))
      result = getVariables();
    else if (firstPart.equalsIgnoreCase("PARENT"))
      result = parent;
    else if (firstPart.equalsIgnoreCase("ROOT")) {
      OCommandContext p = this;
      while (p.getParent() != null)
        p = p.getParent();
      result = p;
    } else {
      if (variables != null && variables.containsKey(firstPart))
        result = variables.get(firstPart);
      else {
        if (child != null)
          result = child.getVariable(firstPart);
        else
          result = getVariableFromParentHierarchy(firstPart);
      }
    }

    if (pos > -1)
      result = ODocumentHelper.getFieldValue(result, lastPart, this);

    return result != null ? result : iDefault;
  }

  protected Object getVariableFromParentHierarchy(String varName) {
    if (this.variables != null && variables.containsKey(varName)) {
      return variables.get(varName);
    }
    if (parent!=null && parent instanceof OBasicCommandContext) {
      return ((OBasicCommandContext) parent).getVariableFromParentHierarchy(varName);
    }
    return null;
  }

  public OCommandContext setVariable(String iName, final Object iValue) {
    if (iName == null)
      return null;

    if (iName.startsWith("$"))
      iName = iName.substring(1);

    init();

    int pos = OStringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
    if (pos > -1) {
      Object nested = getVariable(iName.substring(0, pos));
      if (nested != null && nested instanceof OCommandContext)
        ((OCommandContext) nested).setVariable(iName.substring(pos + 1), iValue);
    } else
      variables.put(iName, iValue);
    return this;
  }

  @Override
  public OCommandContext incrementVariable(String iName) {
    if (iName != null) {
      if (iName.startsWith("$"))
        iName = iName.substring(1);

      init();

      int pos = OStringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
      if (pos > -1) {
        Object nested = getVariable(iName.substring(0, pos));
        if (nested != null && nested instanceof OCommandContext)
          ((OCommandContext) nested).incrementVariable(iName.substring(pos + 1));
      } else {
        final Object v = variables.get(iName);
        if (v == null)
          variables.put(iName, 1);
        else if (v instanceof Number)
          variables.put(iName, OType.increment((Number) v, 1));
        else
          throw new IllegalArgumentException("Variable '" + iName + "' is not a number, but: " + v.getClass());
      }
    }
    return this;
  }

  public long updateMetric(final String iName, final long iValue) {
    if (!recordMetrics)
      return -1;

    init();
    Long value = (Long) variables.get(iName);
    if (value == null)
      value = iValue;
    else
      value = new Long(value.longValue() + iValue);
    variables.put(iName, value);
    return value.longValue();
  }

  /**
   * Returns a read-only map with all the variables.
   */
  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    if (child != null)
      map.putAll(child.getVariables());

    if (variables != null)
      map.putAll(variables);

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
      if (parent != null)
        parent.setChild(this);
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

  private void init() {
    if (variables == null)
      variables = new HashMap<String, Object>();
  }

}
