/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.command.traverse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

public class OTraverseContext implements OCommandContext {
  private OCommandContext                   nestedStack;
  private Set<ORID>                         history = new HashSet<ORID>();
  private List<OTraverseAbstractProcess<?>> stack   = new ArrayList<OTraverseAbstractProcess<?>>();
  private int                               depth   = -1;

  public void push(final OTraverseAbstractProcess<?> iProcess) {
    stack.add(iProcess);
  }

  public OTraverseAbstractProcess<?> pop() {
    if (stack.isEmpty())
      throw new IllegalStateException("Traverse stack is empty");
    return stack.remove(stack.size() - 1);
  }

  public OTraverseAbstractProcess<?> peek() {
    return stack.isEmpty() ? null : stack.get(stack.size() - 1);
  }

  public OTraverseAbstractProcess<?> peek(final int iFromLast) {
    return stack.size() + iFromLast < 0 ? null : stack.get(stack.size() + iFromLast);
  }

  public void reset() {
    stack.clear();
  }

  public boolean isAlreadyTraversed(final OIdentifiable identity) {
    return history.contains(identity.getIdentity());
  }

  public void addTraversed(final OIdentifiable identity) {
    history.add(identity.getIdentity());
  }

  public int incrementDepth() {
    return ++depth;
  }

  public int decrementDepth() {
    return --depth;
  }

  public Object getVariable(final String iName) {
    final String name = iName.trim().toUpperCase();

    if ("DEPTH".startsWith(name))
      return depth;
    else if (name.startsWith("PATH"))
      return ODocumentHelper.getFieldValue(getPath(), iName.substring("PATH".length()));
    else if (name.startsWith("STACK"))
      return ODocumentHelper.getFieldValue(stack, iName.substring("STACK".length()));
    else if (name.startsWith("HISTORY"))
      return ODocumentHelper.getFieldValue(history, iName.substring("HISTORY".length()));
    else if (nestedStack != null)
      // DELEGATE
      nestedStack.getVariable(iName);
    return null;
  }

  public void setVariable(final String iName, final Object iValue) {
    if (nestedStack != null)
      // DELEGATE
      nestedStack.setVariable(iName, iValue);
  }

  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("depth", depth);
    map.put("path", getPath());
    map.put("stack", stack);
    if (nestedStack != null)
      // DELEGATE
      map.putAll(nestedStack.getVariables());
    return map;
  }

  public void merge(final OCommandContext context) {
    nestedStack = context;
  }

  public String getPath() {
    final StringBuilder buffer = new StringBuilder();
    for (OTraverseAbstractProcess<?> process : stack) {
      final String status = process.getStatus();

      if (status != null) {
        if (buffer.length() > 0)
          buffer.append('.');
        buffer.append(status);
      }
    }
    return buffer.toString();
  }

  @Override
  public String toString() {
    return getVariables().toString();
  }
}