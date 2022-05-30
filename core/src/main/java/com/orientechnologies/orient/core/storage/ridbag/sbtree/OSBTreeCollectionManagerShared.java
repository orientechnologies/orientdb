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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.BTreeBonsaiGlobal;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** @author Artem Orobets (enisher-at-gmail.com) */
public final class OSBTreeCollectionManagerShared
    implements OSBTreeCollectionManager, OOrientStartupListener, OOrientShutdownListener {
  public static final String FILE_EXTENSION = ".grb";
  public static final String FILE_NAME_PREFIX = "global_collection_";

  private final OAbstractPaginatedStorage storage;

  private final ConcurrentHashMap<Integer, BTree> fileIdBTreeMap = new ConcurrentHashMap<>();

  private final AtomicLong ridBagIdCounter = new AtomicLong();

  public OSBTreeCollectionManagerShared(OAbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  public void load() {
    final OWriteCache writeCache = storage.getWriteCache();

    for (final Map.Entry<String, Long> entry : writeCache.files().entrySet()) {
      final String fileName = entry.getKey();
      if (fileName.endsWith(FILE_EXTENSION) && fileName.startsWith(FILE_NAME_PREFIX)) {
        final BTree bTree =
            new BTree(
                storage,
                fileName.substring(0, fileName.length() - FILE_EXTENSION.length()),
                FILE_EXTENSION);
        bTree.load();
        fileIdBTreeMap.put(OAbstractWriteCache.extractFileId(entry.getValue()), bTree);
        final EdgeKey edgeKey = bTree.firstKey();

        if (edgeKey != null && edgeKey.ridBagId < 0 && ridBagIdCounter.get() < -edgeKey.ridBagId) {
          ridBagIdCounter.set(-edgeKey.ridBagId);
        }
      }
    }
  }

  public void migrate() throws IOException {
    final OWriteCache writeCache = storage.getWriteCache();
    final Map<String, Long> files = writeCache.files();

    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    final List<String> filesToMigrate = new ArrayList<>();
    for (final Map.Entry<String, Long> entry : files.entrySet()) {
      final String name = entry.getKey();
      if (name.startsWith("collections_") && name.endsWith(".sbc")) {
        filesToMigrate.add(name);
      }
    }

    if (!filesToMigrate.isEmpty()) {
      OLogManager.instance()
          .infoNoDb(
              this,
              "There are found %d RidBags "
                  + "(containers for edges which are going to be migrated). "
                  + "PLEASE DO NOT SHUTDOWN YOUR DATABASE DURING MIGRATION BECAUSE THAT RISKS TO DAMAGE YOUR DATA !!!",
              filesToMigrate.size());
    } else {
      return;
    }

    int migrationCounter = 0;
    for (String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      OLogManager.instance()
          .infoNoDb(
              this,
              "Migration of RidBag for cluster #%s is started ... "
                  + "PLEASE WAIT FOR COMPLETION !",
              clusterIdStr);
      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      atomicOperationsManager.executeInsideAtomicOperation(null, bTree::create);

      final OSBTreeBonsaiLocal<OIdentifiable, Integer> bonsaiLocal =
          new OSBTreeBonsaiLocal<>(
              fileName.substring(0, fileName.length() - ".sbc".length()), ".sbc", storage);
      bonsaiLocal.load(OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);

      final List<OBonsaiBucketPointer> roots = bonsaiLocal.loadRoots();
      for (final OBonsaiBucketPointer root : roots) {
        bonsaiLocal.forEachItem(
            root,
            pair -> {
              try {
                atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                      final ORID rid = pair.first.getIdentity();

                      bTree.put(
                          atomicOperation,
                          new EdgeKey(
                              (root.getPageIndex() << 16) + root.getPageOffset(),
                              rid.getClusterId(),
                              rid.getClusterPosition()),
                          pair.second);
                    });
              } catch (final IOException e) {
                throw OException.wrapException(
                    new OStorageException("Error during migration of RidBag data"), e);
              }
            });
      }

      migrationCounter++;
      OLogManager.instance()
          .infoNoDb(
              this,
              "%d RidBags out of %d are migrated ... PLEASE WAIT FOR COMPLETION !",
              migrationCounter,
              filesToMigrate.size());
    }

    OLogManager.instance()
        .infoNoDb(this, "All RidBags are going to be flushed out ... PLEASE WAIT FOR COMPLETION !");
    final OReadCache readCache = storage.getReadCache();

    int flushCounter = 0;
    for (final String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      final String newFileName = generateLockName(clusterId);

      final long fileId = writeCache.fileIdByName(fileName);
      final long newFileId = writeCache.fileIdByName(newFileName);

      readCache.closeFile(fileId, false, writeCache);
      readCache.closeFile(newFileId, true, writeCache);

      // old file is removed and id of this file is used as id of the new file
      // new file keeps the same name
      writeCache.replaceFileId(fileId, newFileId);
      flushCounter++;

      OLogManager.instance()
          .infoNoDb(
              this,
              "%d RidBags are flushed out of %d ... PLEASE WAIT FOR COMPLETION !",
              flushCounter,
              filesToMigrate.size());
    }

    for (final String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      bTree.load();

      fileIdBTreeMap.put(OWOWCache.extractFileId(bTree.getFileId()), bTree);
    }

    OLogManager.instance().infoNoDb(this, "All RidBags are migrated.");
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(
      final OAtomicOperation atomicOperation, final int clusterId) {
    return doCreateRidBag(atomicOperation, clusterId);
  }

  public boolean isComponentPresent(final OAtomicOperation operation, final int clusterId) {
    return operation.fileIdByName(generateLockName(clusterId)) >= 0;
  }

  public void createComponent(final OAtomicOperation operation, final int clusterId) {
    // lock is already acquired on storage level, during storage open

    final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
    bTree.create(operation);

    final int intFileId = OWOWCache.extractFileId(bTree.getFileId());
    fileIdBTreeMap.put(intFileId, bTree);
  }

  public void deleteComponentByClusterId(
      final OAtomicOperation atomicOperation, final int clusterId) {
    // lock is already acquired on storage level, during cluster drop

    final long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));
    final int intFileId = OAbstractWriteCache.extractFileId(fileId);
    final BTree bTree = fileIdBTreeMap.remove(intFileId);
    if (bTree != null) {
      bTree.delete(atomicOperation);
    }
  }

  private BTreeBonsaiGlobal doCreateRidBag(OAtomicOperation atomicOperation, int clusterId) {
    long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));

    // lock is already acquired on storage level, during start fo the transaction so we
    // are thread safe here.
    if (fileId < 0) {
      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      bTree.create(atomicOperation);

      fileId = bTree.getFileId();
      final long nextRidBagId = -ridBagIdCounter.incrementAndGet();

      final int intFileId = OAbstractWriteCache.extractFileId(fileId);
      fileIdBTreeMap.put(intFileId, bTree);

      return new BTreeBonsaiGlobal(
          bTree, intFileId, nextRidBagId, OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);
    } else {
      final int intFileId = OAbstractWriteCache.extractFileId(fileId);
      final BTree bTree = fileIdBTreeMap.get(intFileId);
      final long nextRidBagId = -ridBagIdCounter.incrementAndGet();

      return new BTreeBonsaiGlobal(
          bTree, intFileId, nextRidBagId, OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);
    }
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(
      OBonsaiCollectionPointer collectionPointer) {
    final int intFileId = OAbstractWriteCache.extractFileId(collectionPointer.getFileId());

    final BTree bTree = fileIdBTreeMap.get(intFileId);

    final long ridBagId;
    final OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    if (rootPointer.getPageIndex() < 0) {
      ridBagId = rootPointer.getPageIndex();
    } else {
      ridBagId = (rootPointer.getPageIndex() << 16) + rootPointer.getPageOffset();
    }

    return new BTreeBonsaiGlobal(
        bTree, intFileId, ridBagId, OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);
  }

  @Override
  public void releaseSBTree(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public void delete(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public OBonsaiCollectionPointer createSBTree(
      int clusterId, OAtomicOperation atomicOperation, UUID ownerUUID) {
    final BTreeBonsaiGlobal bonsaiGlobal = doCreateRidBag(atomicOperation, clusterId);
    final OBonsaiCollectionPointer pointer = bonsaiGlobal.getCollectionPointer();

    if (ownerUUID != null) {
      Map<UUID, OBonsaiCollectionPointer> changedPointers =
          ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return pointer;
  }

  /** Change UUID to null to prevent its serialization to disk. */
  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final OBonsaiCollectionPointer pointer = collection.getPointer();
      ODatabaseDocumentInternal session = ODatabaseRecordThreadLocal.instance().get();
      Map<UUID, OBonsaiCollectionPointer> changedPointers = session.getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {}

  @Override
  public void clearPendingCollections() {}

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
  }

  @Override
  public void clearChangedIds() {
    ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges().clear();
  }

  @Override
  public void onShutdown() {}

  @Override
  public void onStartup() {}

  public void close() {
    fileIdBTreeMap.clear();
  }

  public boolean delete(
      OAtomicOperation atomicOperation, OBonsaiCollectionPointer collectionPointer) {
    final int fileId = (int) collectionPointer.getFileId();
    final BTree bTree = fileIdBTreeMap.get(fileId);
    if (bTree == null) {
      throw new OStorageException(
          "RidBug for with collection pointer " + collectionPointer + " does not exist");
    }

    final long ridBagId;
    final OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    if (rootPointer.getPageIndex() < 0) {
      ridBagId = rootPointer.getPageIndex();
    } else {
      ridBagId = (rootPointer.getPageIndex() << 16) + rootPointer.getPageOffset();
    }

    try (Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }

    return true;
  }

  /**
   * Generates a lock name for the given cluster ID.
   *
   * @param clusterId the cluster ID to generate the lock name for.
   * @return the generated lock name.
   */
  public static String generateLockName(int clusterId) {
    return FILE_NAME_PREFIX + clusterId + FILE_EXTENSION;
  }
}
