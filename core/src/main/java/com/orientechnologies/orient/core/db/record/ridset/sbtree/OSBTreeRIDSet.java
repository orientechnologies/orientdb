/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
// TODO implement ORecordLazyMultiValue
public class OSBTreeRIDSet implements Set<OIdentifiable>, OStringBuilderSerializable {
  private final String                    fileName;
  private final long                      rootIndex;

  private OSBTree<OIdentifiable, Boolean> tree;

  public OSBTreeRIDSet() {
    tree = OSBTreeCollectionManager.INSTANCE.createSBTree();

    fileName = tree.getName();
    rootIndex = tree.getRootIndex();
  }

  public OSBTreeRIDSet(String fileName, long rootIndex) {
    tree = OSBTreeCollectionManager.INSTANCE.loadSBTree(fileName, rootIndex);

    this.fileName = tree.getName();
    this.rootIndex = tree.getRootIndex();
  }

  protected String getFileName() {
    return fileName;
  }

  protected long getRootIndex() {
    return rootIndex;
  }

  @Override
  public int size() {
    return (int) tree.size();
  }

  @Override
  public boolean isEmpty() {
    return tree.size() == 0L;
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof OIdentifiable && contains((OIdentifiable) o);
  }

  public boolean contains(OIdentifiable o) {
    return tree.get(o) != null;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new TreeKeyIterator(tree);
  }

  @Override
  public Object[] toArray() {
    // TODO replace with more efficient implementation

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      list.add(identifiable);
    }

    return list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    // TODO replace with more efficient implementation.

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      list.add(identifiable);
    }

    return list.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    // TODO check if we can avoid get operation
    if (tree.get(oIdentifiable) != null)
      return false;

    tree.put(oIdentifiable, Boolean.TRUE);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof OIdentifiable && remove((OIdentifiable) o);
  }

  public boolean remove(OIdentifiable o) {
    return tree.remove(o) != null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object e : c)
      if (!contains(e))
        return false;
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    boolean modified = false;
    for (OIdentifiable e : c)
      if (add(e))
        modified = true;
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<OIdentifiable> it = iterator();
    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;
    for (Object o : c) {
      modified |= remove(o);
    }

    return modified;
  }

  @Override
  public void clear() {
    tree.clear();
  }

  @Override
  public OStringBuilderSerializable toStream(StringBuilder iOutput) throws OSerializationException {
    throw new UnsupportedOperationException("unimplemented yet");
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder iInput) throws OSerializationException {
    throw new UnsupportedOperationException("unimplemented yet");
  }

  private static class TreeKeyIterator implements Iterator<OIdentifiable> {
    private OSBTreeMapEntryIterator<OIdentifiable, Boolean> entryIterator;

    public TreeKeyIterator(OSBTree<OIdentifiable, Boolean> tree) {
      entryIterator = new OSBTreeMapEntryIterator<OIdentifiable, Boolean>(tree);
    }

    @Override
    public boolean hasNext() {
      return entryIterator.hasNext();
    }

    @Override
    public OIdentifiable next() {
      return entryIterator.next().getKey();
    }

    @Override
    public void remove() {
      entryIterator.remove();
    }
  }

}
