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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;

/**
 * Persistent Set<OIdentifiable> implementation that uses the MVRB-Tree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSet implements Set<OIdentifiable> {
  private final String                    fileName;
  private final long                      rootIndex;

  private OSBTree<OIdentifiable, Boolean> tree;

  public OSBTreeRIDSet() {
    tree = OSBTreeCollectionManager.INSTANCE.createSBTree();

    fileName = tree.getName();
    rootIndex = tree.getRootIndex();
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
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean contains(Object o) {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new Iterator<OIdentifiable>() {
      private OSBTreeMapEntryIterator<OIdentifiable, Boolean> entryIterator = new OSBTreeMapEntryIterator<OIdentifiable, Boolean>(
                                                                                tree);

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
    };
  }

  @Override
  public Object[] toArray() {
    return new Object[0]; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null; // To change body of implemented methods use File | Settings | File Templates.
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
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false; // To change body of implemented methods use File | Settings | File Templates.
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
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void clear() {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  // public OSBTreeRIDSet(String fileId, long rootIndex) {
  // this.fileId = fileId;
  // this.rootIndex = rootIndex;
  //
  // tree = OSBTreeCollectionManager.INSTANCE.loadSBTree(fileId, rootIndex);
  // }

}
