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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OIndexRIDContainerSBTree implements Set<OIdentifiable> {
  public static final String                         INDEX_FILE_EXTENSION = ".irs";
  private OSBTreeBonsaiLocal<OIdentifiable, Boolean> tree;

  protected static final OProfilerMBean              PROFILER             = Orient.instance().getProfiler();

  public OIndexRIDContainerSBTree(long fileId, boolean durableMode) {
    tree = new OSBTreeBonsaiLocal<OIdentifiable, Boolean>(INDEX_FILE_EXTENSION, durableMode);

    tree.create(fileId, OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE);
  }

  public OIndexRIDContainerSBTree(long fileId, OBonsaiBucketPointer rootPointer, boolean durableMode) {
    tree = new OSBTreeBonsaiLocal<OIdentifiable, Boolean>(INDEX_FILE_EXTENSION, durableMode);
    tree.load(fileId, rootPointer, (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying());
  }

  public OIndexRIDContainerSBTree(String file, OBonsaiBucketPointer rootPointer, boolean durableMode) {
    tree = new OSBTreeBonsaiLocal<OIdentifiable, Boolean>(INDEX_FILE_EXTENSION, durableMode);

    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying();
    final long fileId;
    try {
      fileId = storage.getDiskCache().openFile(file + INDEX_FILE_EXTENSION);
    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + file, e);
    }
    tree.load(fileId, rootPointer, storage);
  }

  public OBonsaiBucketPointer getRootPointer() {
    return tree.getRootBucketPointer();
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
    return new TreeKeyIterator(tree, false);
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
    return this.tree.put(oIdentifiable, Boolean.TRUE);
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
    for (Object e : c) {
        if (!contains(e)) {
            return false;
        }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    boolean modified = false;
    for (OIdentifiable e : c) {
        if (add(e)) {
            modified = true;
        }
    }
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

  public void delete() {
    tree.delete();
  }

  public String getName() {
    return tree.getName();
  }

  private static class TreeKeyIterator implements Iterator<OIdentifiable> {
    private final boolean                                   autoConvertToRecord;
    private OSBTreeMapEntryIterator<OIdentifiable, Boolean> entryIterator;

    public TreeKeyIterator(OTreeInternal<OIdentifiable, Boolean> tree, boolean autoConvertToRecord) {
      entryIterator = new OSBTreeMapEntryIterator<OIdentifiable, Boolean>(tree);
      this.autoConvertToRecord = autoConvertToRecord;
    }

    @Override
    public boolean hasNext() {
      return entryIterator.hasNext();
    }

    @Override
    public OIdentifiable next() {
      final OIdentifiable identifiable = entryIterator.next().getKey();
      if (autoConvertToRecord) {
          return identifiable.getRecord();
      } else {
          return identifiable;
      }
    }

    @Override
    public void remove() {
      entryIterator.remove();
    }
  }
}
