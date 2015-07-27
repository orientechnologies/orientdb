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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OIndexRIDContainer implements Set<OIdentifiable> {
  public static final String INDEX_FILE_EXTENSION = ".irs";

  private final long         fileId;
  private Set<OIdentifiable> underlying;
  private boolean            isEmbedded;
  private int                topThreshold         = OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD
                                                      .getValueAsInteger();
  private int                bottomThreshold      = OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD
                                                      .getValueAsInteger();
  private final boolean      durableNonTxMode;

  /**
   * Should be called inside of lock to ensure uniqueness of entity on disk !!!
   */
  public OIndexRIDContainer(String name, boolean durableNonTxMode) {
    fileId = resolveFileIdByName(name + INDEX_FILE_EXTENSION);
    underlying = new HashSet<OIdentifiable>();
    isEmbedded = true;
    this.durableNonTxMode = durableNonTxMode;
  }

  public OIndexRIDContainer(String fileName, Set<OIdentifiable> underlying, boolean autoConvert, boolean durableNonTxMode) {
    this.fileId = resolveFileIdByName(fileName + INDEX_FILE_EXTENSION);
    this.underlying = underlying;
    isEmbedded = !(underlying instanceof OIndexRIDContainerSBTree);
    if (!autoConvert) {
      assert !isEmbedded;
      topThreshold = -1;
      bottomThreshold = -1;
    }

    this.durableNonTxMode = durableNonTxMode;
  }

  private long resolveFileIdByName(String fileName) {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying();
    try {
      final OAtomicOperation atomicOperation = storage.getAtomicOperationsManager().startAtomicOperation(fileName, true);
      final OReadCache readCache = storage.getReadCache();
      final OWriteCache writeCache = storage.getWriteCache();

      if (atomicOperation == null) {
        if (writeCache.exists(fileName))
          return readCache.openFile(fileName, writeCache);

        return readCache.addFile(fileName, writeCache);
      } else {
        long fileId;

        if (atomicOperation.isFileExists(fileName))
          fileId = atomicOperation.openFile(fileName);
        else
          fileId = atomicOperation.addFile(fileName);

        storage.getAtomicOperationsManager().endAtomicOperation(false, null);
        return fileId;
      }
    } catch (IOException e) {
      try {
        storage.getAtomicOperationsManager().endAtomicOperation(true, e);
      } catch (IOException ioe) {
        throw new OSBTreeException("Error of rollback of atomic operation", ioe);
      }

      throw new OSBTreeException("Error creation of sbtree with name " + fileName, e);
    }
  }

  public OIndexRIDContainer(long fileId, Set<OIdentifiable> underlying, boolean durableNonTxMode) {
    this.fileId = fileId;
    this.underlying = underlying;
    isEmbedded = !(underlying instanceof OIndexRIDContainerSBTree);
    this.durableNonTxMode = durableNonTxMode;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public int size() {
    return underlying.size();
  }

  @Override
  public boolean isEmpty() {
    return underlying.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return underlying.contains(o);
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return underlying.iterator();
  }

  @Override
  public Object[] toArray() {
    return underlying.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return underlying.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    final boolean res = underlying.add(oIdentifiable);
    checkTopThreshold();
    return res;
  }

  @Override
  public boolean remove(Object o) {
    final boolean res = underlying.remove(o);
    checkBottomThreshold();
    return res;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return underlying.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    final boolean res = underlying.addAll(c);
    checkTopThreshold();
    return res;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return underlying.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    final boolean res = underlying.removeAll(c);
    checkBottomThreshold();
    return res;
  }

  @Override
  public void clear() {
    if (isEmbedded)
      underlying.clear();
    else {
      final OIndexRIDContainerSBTree tree = (OIndexRIDContainerSBTree) underlying;
      tree.delete();
      underlying = new HashSet<OIdentifiable>();
      isEmbedded = true;
    }
  }

  public boolean isEmbedded() {
    return isEmbedded;
  }

  public boolean isDurableNonTxMode() {
    return durableNonTxMode;
  }

  public Set<OIdentifiable> getUnderlying() {
    return underlying;
  }

  private void checkTopThreshold() {
    if (isEmbedded && topThreshold < underlying.size())
      convertToSbTree();
  }

  private void checkBottomThreshold() {
    if (!isEmbedded && bottomThreshold > underlying.size())
      convertToEmbedded();
  }

  private void convertToEmbedded() {
    final OIndexRIDContainerSBTree tree = (OIndexRIDContainerSBTree) underlying;

    final Set<OIdentifiable> set = new HashSet<OIdentifiable>(tree);

    tree.delete();
    underlying = set;
    isEmbedded = true;
  }

  /**
   * If set is embedded convert it not embedded representation.
   */
  public void checkNotEmbedded() {
    if (isEmbedded)
      convertToSbTree();
  }

  private void convertToSbTree() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
    final OIndexRIDContainerSBTree tree = new OIndexRIDContainerSBTree(fileId, durableNonTxMode, (OAbstractPaginatedStorage) db
        .getStorage().getUnderlying());

    tree.addAll(underlying);

    underlying = tree;
    isEmbedded = false;
  }
}
