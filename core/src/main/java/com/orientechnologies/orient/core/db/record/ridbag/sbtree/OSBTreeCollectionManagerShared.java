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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerShared implements OCloseable, OSBTreeCollectionManager {
  private final int                                                                  evictionThreshold;
  private final int                                                                  cacheMaxSize;

  private static final String                                                        FILE_NAME         = "sbtreeridbag";

  private final int                                                                  shift;
  private final int                                                                  mask;
  private final Object[]                                                             locks;
  private volatile long                                                              fileId            = -1;

  public final String                                                                DEFAULT_EXTENSION = ".sbc";
  private final ConcurrentLinkedHashMap<OBonsaiBucketPointer, SBTreeBonsaiContainer> treeCache         = new ConcurrentLinkedHashMap.Builder<OBonsaiBucketPointer, SBTreeBonsaiContainer>()
                                                                                                           .maximumWeightedCapacity(
                                                                                                               Long.MAX_VALUE)
                                                                                                           .build();

  public OSBTreeCollectionManagerShared() {
    this(OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger(),
        OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger());
  }

  public OSBTreeCollectionManagerShared(int evictionThreshold, int cacheMaxSize) {
    this.evictionThreshold = evictionThreshold;
    this.cacheMaxSize = cacheMaxSize;

    final int concurrencyLevel = Runtime.getRuntime().availableProcessors() * 4;
    int cL = 1;

    int sh = 0;
    while (cL < concurrencyLevel) {
      cL <<= 1;
      sh++;
    }

    shift = 32 - sh;
    mask = cL - 1;

    final Object[] locks = new Object[cL];
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new Object();
    }

    this.locks = locks;
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createSBTree() {
    OSBTreeBonsai<OIdentifiable, Integer> tree = new OSBTreeBonsai<OIdentifiable, Integer>(DEFAULT_EXTENSION, true);

    tree.create(FILE_NAME, OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());

    final Object lock = treesSubsetLock(tree.getRootBucketPointer());
    synchronized (lock) {
      SBTreeBonsaiContainer container = new SBTreeBonsaiContainer(tree);
      treeCache.put(tree.getRootBucketPointer(), container);

      container.usagesCounter++;
    }

    if (fileId == -1)
      fileId = tree.getFileId();

    return tree;
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiBucketPointer rootIndex) {
    try {
      if (fileId == -1) {
        OStorageLocalAbstract storage = (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
            .getUnderlying();
        fileId = storage.getDiskCache().openFile(FILE_NAME + DEFAULT_EXTENSION);
      }
    } catch (IOException e) {
      throw new OStorageException("Can not open file with name '" + FILE_NAME + DEFAULT_EXTENSION + "'", e);
    }

    final Object lock = treesSubsetLock(rootIndex);

    OSBTreeBonsai<OIdentifiable, Integer> tree;
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.remove(rootIndex);
      if (container != null) {
        container.usagesCounter++;
        tree = container.tree;
      } else {
        tree = new OSBTreeBonsai<OIdentifiable, Integer>(DEFAULT_EXTENSION, true);
        tree.load(fileId, rootIndex, (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());

        assert tree.getRootBucketPointer().equals(rootIndex);

        container = new SBTreeBonsaiContainer(tree);
        container.usagesCounter++;
      }

      treeCache.put(rootIndex, container);
    }

    evict();

    return tree;
  }

  @Override
  public void releaseSBTree(OBonsaiBucketPointer rootIndex) {
    final Object lock = treesSubsetLock(rootIndex);
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.get(rootIndex);
      assert container != null;
      container.usagesCounter--;
      assert container.usagesCounter >= 0;
    }

    evict();
  }

  @Override
  public void delete(OBonsaiBucketPointer rootIndex) {
    final Object lock = treesSubsetLock(rootIndex);
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.get(rootIndex);
      assert container != null;

      if (container.usagesCounter != 0)
        throw new IllegalStateException("Can not delete SBTreeBonsai instance because it is used in other thread.");

      treeCache.remove(rootIndex);
    }
  }

  private void evict() {
    if (treeCache.size() <= cacheMaxSize)
      return;

    for (OBonsaiBucketPointer rootPointer : treeCache.ascendingKeySetWithLimit(evictionThreshold)) {
      final Object treeLock = treesSubsetLock(rootPointer);
      synchronized (treeLock) {
        SBTreeBonsaiContainer container = treeCache.get(rootPointer);
        if (container != null && container.usagesCounter == 0)
          treeCache.remove(rootPointer);
      }
    }
  }

  @Override
  public void close() {
    treeCache.clear();
  }

  int size() {
    return treeCache.size();
  }

  private Object treesSubsetLock(OBonsaiBucketPointer rootIndex) {
    final int hashCode = rootIndex.hashCode();
    final int index = (hashCode >>> shift) & mask;

    return locks[index];
  }

  private static final class SBTreeBonsaiContainer {
    private final OSBTreeBonsai<OIdentifiable, Integer> tree;
    private int                                         usagesCounter = 0;

    private SBTreeBonsaiContainer(OSBTreeBonsai<OIdentifiable, Integer> tree) {
      this.tree = tree;
    }
  }
}
