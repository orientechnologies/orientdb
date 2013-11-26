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

import java.util.Iterator;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManager {
  private static final int                                                           eviction_threshold = 100;
  private static final int                                                           CACHE_MAX_SIZE     = 100000;

  private static final String                                                        FILE_NAME          = "sbtreeridset";

  private final int                                                                  shift;
  private final int                                                                  mask;
  private final Object[]                                                             locks;

  public final String                                                                DEFAULT_EXTENSION  = ".sbc";
  private final ConcurrentLinkedHashMap<OBonsaiBucketPointer, SBTreeBonsaiContainer> treeCache          = new ConcurrentLinkedHashMap.Builder<OBonsaiBucketPointer, SBTreeBonsaiContainer>()
                                                                                                            .maximumWeightedCapacity(
                                                                                                                Long.MAX_VALUE)
                                                                                                            .build();

  public OSBTreeCollectionManager() {
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

  public OSBTreeBonsai<OIdentifiable, Boolean> createSBTree() {
    OSBTreeBonsai<OIdentifiable, Boolean> tree = new OSBTreeBonsai<OIdentifiable, Boolean>(DEFAULT_EXTENSION, true);

    tree.create(FILE_NAME, OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());

    final Object lock = treesSubsetLock(tree.getRootBucketPointer());
    synchronized (lock) {
      SBTreeBonsaiContainer container = new SBTreeBonsaiContainer(tree);
      SBTreeBonsaiContainer oldContainer = treeCache.put(tree.getRootBucketPointer(), new SBTreeBonsaiContainer(tree));
      assert oldContainer == null;

      container.usagesCounter++;
    }

    return tree;
  }

  public OSBTreeBonsai<OIdentifiable, Boolean> loadSBTree(OBonsaiBucketPointer rootIndex) {
    final Object lock = treesSubsetLock(rootIndex);

    OSBTreeBonsai<OIdentifiable, Boolean> tree;
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.remove(rootIndex);
      if (container != null) {
        container.usagesCounter++;
        tree = container.tree;
      } else {
        tree = new OSBTreeBonsai<OIdentifiable, Boolean>(DEFAULT_EXTENSION, true);
        tree.load(FILE_NAME, rootIndex, (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
            .getUnderlying());

        assert tree.getRootBucketPointer().equals(rootIndex);

        container = new SBTreeBonsaiContainer(tree);
        container.usagesCounter++;
      }

      treeCache.put(rootIndex, container);
    }

    evict();

    return tree;
  }

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

  private void evict() {
    if (treeCache.size() <= CACHE_MAX_SIZE)
      return;

    Iterator<OBonsaiBucketPointer> iterator = treeCache.keySet().iterator();
    while (treeCache.size() > CACHE_MAX_SIZE - eviction_threshold && iterator.hasNext()) {
      final OBonsaiBucketPointer rootPointer = iterator.next();
      final Object treeLock = treesSubsetLock(rootPointer);
      synchronized (treeLock) {
        SBTreeBonsaiContainer container = treeCache.get(rootPointer);
        if (container != null && container.usagesCounter == 0)
          treeCache.remove(rootPointer);
      }
    }
  }

  public void startup() {

  }

  public void shutdown() {
    treeCache.clear();
  }

  private Object treesSubsetLock(OBonsaiBucketPointer rootIndex) {
    final int hashCode = rootIndex.hashCode();
    final int index = (hashCode >>> shift) & mask;

    return locks[index];
  }

  private static final class SBTreeBonsaiContainer {
    private final OSBTreeBonsai<OIdentifiable, Boolean> tree;
    private int                                         usagesCounter = 0;

    private SBTreeBonsaiContainer(OSBTreeBonsai<OIdentifiable, Boolean> tree) {
      this.tree = tree;
    }
  }
}
