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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OIndexRIDContainer implements Set<OIdentifiable> {
  public static final String INDEX_FILE_EXTENSION = ".irs";

  private final long         fileId;
  private Set<OIdentifiable> underlying;
  private boolean            isEmbedded;
  private int                topThreshold         = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD
                                                      .getValueAsInteger();
  private int                bottomThreshold      = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD
                                                      .getValueAsInteger();

  public OIndexRIDContainer(String name) {
    fileId = resolveFileIdByName(name + INDEX_FILE_EXTENSION);
    underlying = new HashSet<OIdentifiable>();
    isEmbedded = true;
  }

  public OIndexRIDContainer(String fileName, Set<OIdentifiable> underlying, boolean autoConvert) {
    this.fileId = resolveFileIdByName(fileName + INDEX_FILE_EXTENSION);
    this.underlying = underlying;
    isEmbedded = !(underlying instanceof OIndexRIDContainerSBTree);
    if (!autoConvert) {
      assert !isEmbedded;
      topThreshold = -1;
      bottomThreshold = -1;
    }
  }

  private long resolveFileIdByName(String fileName) {
    final OStorageLocalAbstract storage = (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying();
    try {
      return storage.getDiskCache().openFile(fileName);
    } catch (IOException e) {
      throw new OSBTreeException("Error creation of sbtree with name" + fileName, e);
    }
  }

  public OIndexRIDContainer(long fileId, Set<OIdentifiable> underlying) {
    this.fileId = fileId;
    this.underlying = underlying;
    isEmbedded = !(underlying instanceof OIndexRIDContainerSBTree);
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
    final OIndexRIDContainerSBTree tree = new OIndexRIDContainerSBTree(fileId);

    tree.addAll(underlying);

    underlying = tree;
    isEmbedded = false;
  }
}
