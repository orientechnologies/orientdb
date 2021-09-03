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

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.storage.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent
 * way.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OIndexRIDContainerSBTree implements Set<OIdentifiable> {
  public static final String INDEX_FILE_EXTENSION = ".irs";

  /**
   * Generates a lock name for the given index name.
   *
   * @param indexName the index name to generate the lock name for.
   * @return the generated lock name.
   */
  public static String generateLockName(String indexName) {
    return indexName + INDEX_FILE_EXTENSION;
  }

  private final OSBTreeBonsaiLocal<OIdentifiable, Boolean> tree;
  private final OAtomicOperationsManager atomicOperationsManager;

  OIndexRIDContainerSBTree(long fileId, OAbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new OSBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);

    try {
      tree.create(atomicOperation, OCompactedLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE);
    } catch (IOException e) {
      throw OException.wrapException(
          new ODatabaseException("Error during creation of index container "), e);
    }
  }

  public OIndexRIDContainerSBTree(
      long fileId, OBonsaiBucketPointer rootPointer, OAbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new OSBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);
    tree.load(rootPointer);
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
    return tree.isEmpty();
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
    final ArrayList<OIdentifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray();
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    final ArrayList<OIdentifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return this.tree.put(atomicOperation, oIdentifiable, Boolean.TRUE);
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof OIdentifiable && remove((OIdentifiable) o);
  }

  public boolean remove(OIdentifiable o) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return tree.remove(atomicOperation, o) != null;
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
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);
    tree.clear(atomicOperation);
  }

  public void delete() {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    tree.delete(atomicOperation);
  }

  public String getName() {
    return tree.getName();
  }

  private static class TreeKeyIterator implements Iterator<OIdentifiable> {
    private final boolean autoConvertToRecord;
    private final OSBTreeMapEntryIterator<OIdentifiable, Boolean> entryIterator;

    private TreeKeyIterator(
        OTreeInternal<OIdentifiable, Boolean> tree, boolean autoConvertToRecord) {
      entryIterator = new OSBTreeMapEntryIterator<>(tree);
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
