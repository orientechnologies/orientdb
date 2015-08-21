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

import java.util.UUID;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public abstract class OSBTreeCollectionManagerAbstract implements OCloseable, OSBTreeCollectionManager {
  public static final String                                                             FILE_NAME_PREFIX  = "collections_";
  public static final String                                                             DEFAULT_EXTENSION = ".sbc";
  protected final int                                                                    evictionThreshold;
  protected final int                                                                    cacheMaxSize;
  protected final int                                                                    shift;
  protected final int                                                                    mask;
  protected final Object[]                                                               locks;
  private final ConcurrentLinkedHashMap<OBonsaiCollectionPointer, SBTreeBonsaiContainer> treeCache         = new ConcurrentLinkedHashMap.Builder<OBonsaiCollectionPointer, SBTreeBonsaiContainer>()
                                                                                                               .maximumWeightedCapacity(
                                                                                                                   Long.MAX_VALUE)
                                                                                                               .build();

  public OSBTreeCollectionManagerAbstract() {
    this(OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger(),
        OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger());
  }

  public OSBTreeCollectionManagerAbstract(int evictionThreshold, int cacheMaxSize) {
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
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(int clusterId) {
    return loadSBTree(createSBTree(clusterId, null));
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID) {
    OSBTreeBonsai<OIdentifiable, Integer> tree = createTree(clusterId);
    return tree.getCollectionPointer();
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer) {
    final Object lock = treesSubsetLock(collectionPointer);

    OSBTreeBonsai<OIdentifiable, Integer> tree;
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.get(collectionPointer);
      if (container != null) {
        container.usagesCounter++;
        tree = container.tree;
      } else {
        tree = loadTree(collectionPointer);

        assert tree.getRootBucketPointer().equals(collectionPointer.getRootPointer());

        container = new SBTreeBonsaiContainer(tree);
        container.usagesCounter++;

				treeCache.put(collectionPointer, container);
      }


    }

    evict();

    return tree;
  }

  @Override
  public void releaseSBTree(OBonsaiCollectionPointer collectionPointer) {
    final Object lock = treesSubsetLock(collectionPointer);
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
      assert container != null;
      container.usagesCounter--;
      assert container.usagesCounter >= 0;
    }

    evict();
  }

  @Override
  public void delete(OBonsaiCollectionPointer collectionPointer) {
    final Object lock = treesSubsetLock(collectionPointer);
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
      assert container != null;

      if (container.usagesCounter != 0)
        throw new IllegalStateException("Cannot delete SBTreeBonsai instance because it is used in other thread.");

      treeCache.remove(collectionPointer);
    }
  }

  private void evict() {
    if (treeCache.size() <= cacheMaxSize)
      return;

    for (OBonsaiCollectionPointer collectionPointer : treeCache.ascendingKeySetWithLimit(evictionThreshold)) {
      final Object treeLock = treesSubsetLock(collectionPointer);
      synchronized (treeLock) {
        SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
        if (container != null && container.usagesCounter == 0)
          treeCache.remove(collectionPointer);
      }
    }
  }

  @Override
  public void close() {
    treeCache.clear();
  }

  protected abstract OSBTreeBonsai<OIdentifiable, Integer> createTree(int clusterId);

  protected abstract OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer);

  int size() {
    return treeCache.size();
  }

  private Object treesSubsetLock(OBonsaiCollectionPointer collectionPointer) {
    final int hashCode = collectionPointer.hashCode();
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
