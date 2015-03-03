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
package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class OTraverseContext extends OBasicCommandContext {
  private Set<ORID> history = new HashSet<ORID>();
  private Memory    memory  = new StackMemory();

  private OTraverseAbstractProcess<?> currentProcess;

  public void push(final OTraverseAbstractProcess<?> iProcess) {
    memory.add(iProcess);
  }

  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("depth", getDepth());
    map.put("path", getPath());
    map.put("stack", memory.getUnderlying());
    // DELEGATE
    map.putAll(super.getVariables());
    return map;
  }

  public Object getVariable(final String iName) {
    final String name = iName.trim().toUpperCase();

    if ("DEPTH".startsWith(name))
      return getDepth();
    else if (name.startsWith("PATH"))
      return ODocumentHelper.getFieldValue(getPath(), iName.substring("PATH".length()));
    else if (name.startsWith("STACK"))
      return ODocumentHelper.getFieldValue(memory.getUnderlying(), iName.substring("STACK".length()));
    else if (name.startsWith("HISTORY"))
      return ODocumentHelper.getFieldValue(history, iName.substring("HISTORY".length()));
    else
      // DELEGATE
      return super.getVariable(iName);
  }

  public void pop() {
    try {
      memory.dropFrame();
    } catch (NoSuchElementException e) {
      throw new IllegalStateException("Traverse stack is empty", e);
    }
  }

  public OTraverseAbstractProcess<?> next() {
    currentProcess = memory.next();
    return currentProcess;
  }

  public boolean isEmpty() {
    return memory.isEmpty();
  }

  public void reset() {
    memory.clear();
  }

  public boolean isAlreadyTraversed(final OIdentifiable identity) {
    return history.contains(identity.getIdentity());
  }

  public void addTraversed(final OIdentifiable identity) {
    history.add(identity.getIdentity());
  }

  public String getPath() {
    return currentProcess == null ? "" : currentProcess.getPath().toString();
  }

  public int getDepth() {
    return currentProcess == null ? 0 : currentProcess.getPath().getDepth();
  }

  public void setStrategy(OTraverse.STRATEGY strategy) {
    if (strategy == OTraverse.STRATEGY.BREADTH_FIRST)
      memory = new QueueMemory(memory);
    else
      memory = new StackMemory(memory);
  }

  private interface Memory {
    void add(OTraverseAbstractProcess<?> iProcess);

    OTraverseAbstractProcess<?> next();

    void dropFrame();

    void clear();

    Collection<OTraverseAbstractProcess<?>> getUnderlying();

    boolean isEmpty();
  }

  private abstract static class AbstractMemory implements Memory {
    protected Deque<OTraverseAbstractProcess<?>> deque = new ArrayDeque<OTraverseAbstractProcess<?>>();

    public AbstractMemory() {
      deque = new ArrayDeque<OTraverseAbstractProcess<?>>();
    }

    public AbstractMemory(Memory memory) {
      deque = new ArrayDeque<OTraverseAbstractProcess<?>>(memory.getUnderlying());
    }

    @Override
    public OTraverseAbstractProcess<?> next() {
      return deque.peek();
    }

    @Override
    public void dropFrame() {
      deque.removeFirst();
    }

    @Override
    public void clear() {
      deque.clear();
    }

    @Override
    public boolean isEmpty() {
      return deque.isEmpty();
    }

    @Override
    public Collection<OTraverseAbstractProcess<?>> getUnderlying() {
      return deque;
    }
  }

  private static class StackMemory extends AbstractMemory {
    public StackMemory() {
      super();
    }

    public StackMemory(Memory memory) {
      super(memory);
    }

    @Override
    public void add(OTraverseAbstractProcess<?> iProcess) {
      deque.push(iProcess);
    }
  }

  private static class QueueMemory extends AbstractMemory {
    public QueueMemory(Memory memory) {
      super(memory);
    }

    @Override
    public void add(OTraverseAbstractProcess<?> iProcess) {
      deque.addLast(iProcess);
    }
  }
}
