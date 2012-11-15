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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map. Supports parent/child context to build a tree
 * of contexts. If a variable is not found on current object the search is applied recursively on child contexts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OBasicCommandContext implements OCommandContext {
  protected boolean             recordMetrics = false;
  protected OCommandContext     parent;
  protected OCommandContext     child;
  protected Map<String, Object> variables;

  public Object getVariable(String iName) {
    if (iName == null)
      return null;

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
          return parent.getVariable(lastPart.substring(1));
        else
          return ODocumentHelper.getFieldValue(parent, lastPart);
      } else if (firstPart.equalsIgnoreCase("ROOT") && parent != null) {
        OCommandContext p = this;
        while (p.getParent() != null)
          p = p.getParent();
        if (lastPart.startsWith("$"))
          return p.getVariable(lastPart.substring(1));
        else
          return ODocumentHelper.getFieldValue(p, lastPart);
      }
    } else {
      firstPart = iName;
      lastPart = null;
    }

    Object result = null;
    if (firstPart.equalsIgnoreCase("CONTEXT"))
      result = getVariables();
    else if (firstPart.equalsIgnoreCase("PARENT"))
      return parent;
    else if (firstPart.equalsIgnoreCase("ROOT")) {
      OCommandContext p = this;
      while (p.getParent() != null)
        p = p.getParent();
      return p;
    } else {
      if (variables != null && variables.containsKey(firstPart))
        result = variables.get(firstPart);
      else if (child != null)
        result = child.getVariable(firstPart);
    }

    if (pos > -1)
      result = ODocumentHelper.getFieldValue(result, lastPart, this);

    return result;
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
      // REMOVE IT
      child.setParent(null);
      child = null;

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
    if (parent != iParentContext)
      parent = iParentContext;

    return this;
  }

  @Override
  public String toString() {
    return getVariables().toString();
  }

  private void init() {
    if (variables == null)
      variables = new HashMap<String, Object>();
  }

  public boolean isRecordingMetrics() {
    return recordMetrics;
  }

  public OCommandContext setRecordingMetrics(final boolean recordMetrics) {
    this.recordMetrics = recordMetrics;
    return this;
  }
}
