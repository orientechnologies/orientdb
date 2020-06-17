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
package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class OTraverseContext extends OBasicCommandContext {
  private Memory memory = new StackMemory();
  private Set<ORID> history = new HashSet<ORID>();

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
    final String name = iName.trim().toUpperCase(Locale.ENGLISH);

    if ("DEPTH".startsWith(name)) return getDepth();
    else if (name.startsWith("PATH"))
      return ODocumentHelper.getFieldValue(getPath(), iName.substring("PATH".length()));
    else if (name.startsWith("STACK")) {

      Object result =
          ODocumentHelper.getFieldValue(memory.getUnderlying(), iName.substring("STACK".length()));
      if (result instanceof ArrayDeque) {
        result = ((ArrayDeque) result).clone();
      }
      return result;
    } else if (name.startsWith("HISTORY"))
      return ODocumentHelper.getFieldValue(history, iName.substring("HISTORY".length()));
    else
      // DELEGATE
      return super.getVariable(iName);
  }

  public void pop(final OIdentifiable currentRecord) {
    if (currentRecord != null) {
      final ORID rid = currentRecord.getIdentity();
      if (!history.remove(rid))
        OLogManager.instance().warn(this, "Element '" + rid + "' not found in traverse history");
    }

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

  public boolean isAlreadyTraversed(final OIdentifiable identity, final int iLevel) {
    if (history.contains(identity.getIdentity())) return true;

    // final int[] l = history.get(identity.getIdentity());
    // if (l == null)
    // return false;
    //
    // for (int i = 0; i < l.length && l[i] > -1; ++i)
    // if (l[i] == iLevel)
    // return true;

    return false;
  }

  public void addTraversed(final OIdentifiable identity, final int iLevel) {
    history.add(identity.getIdentity());

    // final int[] l = history.get(identity.getIdentity());
    // if (l == null) {
    // final int[] array = new int[BUCKET_SIZE];
    // array[0] = iLevel;
    // Arrays.fill(array, 1, BUCKET_SIZE, -1);
    // history.put(identity.getIdentity(), array);
    // } else {
    // if (l[l.length - 1] > -1) {
    // // ARRAY FULL, ENLARGE IT
    // final int[] array = Arrays.copyOf(l, l.length + BUCKET_SIZE);
    // array[l.length] = iLevel;
    // Arrays.fill(array, l.length + 1, array.length, -1);
    // history.put(identity.getIdentity(), array);
    // } else {
    // for (int i = l.length - 2; i >= 0; --i) {
    // if (l[i] > -1) {
    // l[i + 1] = iLevel;
    // break;
    // }
    // }
    // }
    // }
  }

  public String getPath() {
    return currentProcess == null ? "" : currentProcess.getPath().toString();
  }

  public int getDepth() {
    return currentProcess == null ? 0 : currentProcess.getPath().getDepth();
  }

  public void setStrategy(final OTraverse.STRATEGY strategy) {
    if (strategy == OTraverse.STRATEGY.BREADTH_FIRST) memory = new QueueMemory(memory);
    else memory = new StackMemory(memory);
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
    protected final Deque<OTraverseAbstractProcess<?>> deque;

    public AbstractMemory() {
      deque = new ArrayDeque<OTraverseAbstractProcess<?>>();
    }

    public AbstractMemory(final Memory memory) {
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

    public StackMemory(final Memory memory) {
      super(memory);
    }

    @Override
    public void add(final OTraverseAbstractProcess<?> iProcess) {
      deque.push(iProcess);
    }
  }

  private static class QueueMemory extends AbstractMemory {
    public QueueMemory(final Memory memory) {
      super(memory);
    }

    @Override
    public void add(final OTraverseAbstractProcess<?> iProcess) {
      deque.addLast(iProcess);
    }

    public ODatabase getDatabase() {
      return ODatabaseRecordThreadLocal.instance().getIfDefined();
    }
  }
}
