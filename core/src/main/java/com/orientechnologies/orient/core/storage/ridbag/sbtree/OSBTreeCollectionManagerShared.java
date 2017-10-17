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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OAccessToSBtreeCollectionManagerIsProhibitedException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerShared extends OSBTreeCollectionManagerAbstract
    implements OOrientStartupListener, OOrientShutdownListener {
  /**
   * Message which is provided during throwing of {@link OAccessToSBtreeCollectionManagerIsProhibitedException}.
   */
  private static final String PROHIBITED_EXCEPTION_MESSAGE = "Access to the manager of RidBags which are based on B-Tree "
      + "implementation is prohibited. Typically it means that you use database under distributed cluster configuration. Please check "
      + "that following setting in your server configuration " + OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD
      .getKey() + " is set to " + Integer.MAX_VALUE;

  private final OAbstractPaginatedStorage storage;
  private volatile ThreadLocal<Map<UUID, OBonsaiCollectionPointer>> collectionPointerChanges = new CollectionPointerChangesThreadLocal();

  /**
   * If this flag is set to {@code true} then all access to the manager will be prohibited and exception
   * {@link OAccessToSBtreeCollectionManagerIsProhibitedException} will be thrown.
   */
  private volatile boolean prohibitAccess = false;

  public OSBTreeCollectionManagerShared(OAbstractPaginatedStorage storage) {
    super(storage);

    this.storage = storage;
  }

  // for testing purposes
  /* internal */ OSBTreeCollectionManagerShared(int evictionThreshold, int cacheMaxSize, OAbstractPaginatedStorage storage) {
    super(storage, evictionThreshold, cacheMaxSize);

    this.storage = storage;
  }

  @Override
  public void onShutdown() {
    collectionPointerChanges = null;
    super.onShutdown();
  }

  @Override
  public void onStartup() {
    super.onStartup();
    if (collectionPointerChanges == null)
      collectionPointerChanges = new CollectionPointerChangesThreadLocal();
  }

  /**
   * Once this method is called any attempt to load/create/delete b-tree will be resulted in exception thrown.
   */
  public void prohibitAccess() {
    prohibitAccess = true;
  }

  private void checkAccess() {
    if (prohibitAccess)
      throw new OAccessToSBtreeCollectionManagerIsProhibitedException(PROHIBITED_EXCEPTION_MESSAGE);
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(int clusterId) {
    checkAccess();

    return super.createAndLoadTree(clusterId);
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer) {
    checkAccess();

    return super.loadSBTree(collectionPointer);
  }

  @Override
  public void delete(OBonsaiCollectionPointer collectionPointer) {
    checkAccess();

    super.delete(collectionPointer);
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID) {
    checkAccess();

    final OBonsaiCollectionPointer pointer = super.createSBTree(clusterId, ownerUUID);

    if (ownerUUID != null) {
      Map<UUID, OBonsaiCollectionPointer> changedPointers = collectionPointerChanges.get();
      if (pointer != null && pointer.isValid())
        changedPointers.put(ownerUUID, pointer);
    }

    return pointer;
  }

  @Override
  protected OSBTreeBonsaiLocal<OIdentifiable, Integer> createTree(int clusterId) {

    OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = new OSBTreeBonsaiLocal<>(FILE_NAME_PREFIX + clusterId, DEFAULT_EXTENSION,
        storage);
    tree.create(OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);

    return tree;
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    String fileName;
    OAtomicOperation atomicOperation = storage.getAtomicOperationsManager().getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(collectionPointer.getFileId());
    } else {
      fileName = atomicOperation.fileNameById(collectionPointer.getFileId());
    }

    OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = new OSBTreeBonsaiLocal<>(
        fileName.substring(0, fileName.length() - DEFAULT_EXTENSION.length()), DEFAULT_EXTENSION, storage);

    if (tree.load(collectionPointer.getRootPointer()))
      return tree;
    else
      return null;
  }

  /**
   * Change UUID to null to prevent its serialization to disk.
   */
  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final OBonsaiCollectionPointer pointer = collection.getPointer();

      Map<UUID, OBonsaiCollectionPointer> changedPointers = collectionPointerChanges.get();
      if (pointer != null && pointer.isValid())
        changedPointers.put(ownerUUID, pointer);
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {
  }

  @Override
  public void clearPendingCollections() {
  }

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return collectionPointerChanges.get();
  }

  @Override
  public void clearChangedIds() {
    collectionPointerChanges.get().clear();
  }

  private static class CollectionPointerChangesThreadLocal extends ThreadLocal<Map<UUID, OBonsaiCollectionPointer>> {
    @Override
    protected Map<UUID, OBonsaiCollectionPointer> initialValue() {
      return new HashMap<>();
    }
  }
}
