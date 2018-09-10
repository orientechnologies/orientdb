package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OClusterPositionAllocator;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class OClusterPositionAllocatorDatabase implements OClusterPositionAllocator {
  private OrientDB context;
  private String   database;

  private volatile AtomicLong[] allocators;

  public OClusterPositionAllocatorDatabase(OrientDB context, String databaseName) {
    this.context = context;
    this.database = databaseName;
    registerListener();
    initAllocators();
  }

  private void registerListener() {
    OrientDBInternal internal = OrientDBInternal.extract(context);
    try (ODatabaseDocumentInternal session = internal.openNoAuthorization(database)) {
      session.getSharedContext().registerListener(new OMetadataUpdateListener() {
        @Override
        public void onSchemaUpdate(String database, OSchemaShared schema) {

        }

        @Override
        public void onIndexManagerUpdate(String database, OIndexManager indexManager) {

        }

        @Override
        public void onFunctionLibraryUpdate(String database) {

        }

        @Override
        public void onSequenceLibraryUpdate(String database) {

        }

        @Override
        public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
          //this may not be perfect now because allocation may have happened in parallel to schema operation now that the schema operations don't go through the coordinator.
          initAllocators();
        }
      });
    }
  }

  @Override
  public synchronized long allocate(int clusterId) {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator
    return allocators[clusterId].incrementAndGet();
  }

  private synchronized void initAllocators() {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator
    OrientDBInternal internal = OrientDBInternal.extract(context);
    try (ODatabaseDocumentInternal session = internal.openNoAuthorization(database)) {

      OStorage storage = session.getStorage().getUnderlying();
      Collection<? extends OCluster> clusterInstances = storage.getClusterInstances();
      allocators = new AtomicLong[clusterInstances.size()];
      for (OCluster cluster : clusterInstances) {
        try {
          allocators[cluster.getId()] = new AtomicLong(cluster.getLastPosition());
        } catch (IOException e) {
          OLogManager.instance().error(this, "error resolving last position", e);
        }
      }
    }
  }

}
