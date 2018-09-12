package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OClusterPositionAllocator;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class OClusterPositionAllocatorDatabase implements OClusterPositionAllocator {
  private OrientDBInternal context;
  private String           database;

  private volatile AtomicLong[] allocators;

  public OClusterPositionAllocatorDatabase(OrientDBInternal context, String databaseName) {
    this.context = context;
    this.database = databaseName;
    registerListener();
  }

  private void registerListener() {
    Orient.instance().addDbLifecycleListener(new ODatabaseLifecycleListener() {
      @Override
      public void onCreate(ODatabaseInternal iDatabase) {

      }

      @Override
      public void onOpen(ODatabaseInternal iDatabase) {
        OStorage storage = iDatabase.getStorage().getUnderlying();
        iDatabase.getSharedContext().registerListener(new OMetadataUpdateListener() {
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
            initAllocators(storage);
          }
        });

        initAllocators(storage);
      }

      @Override
      public void onClose(ODatabaseInternal iDatabase) {

      }

      @Override
      public void onDrop(ODatabaseInternal iDatabase) {

      }

      @Override
      public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {

      }
    });
  }

  @Override
  public synchronized long allocate(int clusterId) {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator
    return allocators[clusterId].getAndIncrement();
  }

  private synchronized void initAllocators(OStorage storage) {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator

    Collection<? extends OCluster> clusterInstances = storage.getClusterInstances();
    allocators = new AtomicLong[clusterInstances.size()];
    for (OCluster cluster : clusterInstances) {
      try {
        long next = cluster.getLastPosition();
        if (next == 0 && cluster.getPhysicalPosition(new OPhysicalPosition(next)) != null) {
          next += 1;
        }
        allocators[cluster.getId()] = new AtomicLong(next);
      } catch (IOException e) {
        OLogManager.instance().error(this, "error resolving last position", e);
      }
    }
  }
}
