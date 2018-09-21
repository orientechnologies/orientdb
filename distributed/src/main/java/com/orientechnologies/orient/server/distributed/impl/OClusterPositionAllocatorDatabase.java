package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OClusterPositionAllocator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class OClusterPositionAllocatorDatabase implements OClusterPositionAllocator {

  private volatile Map<String, Integer> names = new HashMap<>();
  private volatile AtomicLong[]         allocators;

  public OClusterPositionAllocatorDatabase(OSharedContext context) {
    registerListener(context);
  }

  private void registerListener(OSharedContext context1) {

    context1.registerListener(new OMetadataUpdateListener() {
      @Override
      public void onSchemaUpdate(String database, OSchemaShared schema) {
        initAllocators(context1.getStorage());
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
        initAllocators(context1.getStorage());
      }
    });

    initAllocators(context1.getStorage());
  }

  @Override
  public synchronized long allocate(int clusterId) {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator
    return allocators[clusterId].getAndIncrement();
  }

  private synchronized void initAllocators(OStorage storage) {
    //Syncrhonized will be replaced when the schema operations will go through the coordinator
    int clusters = storage.getClusters();
    Collection<? extends OCluster> clusterInstances = storage.getClusterInstances();
    AtomicLong[] newAllocators = new AtomicLong[clusters];
    Map<String, Integer> newNames = new HashMap<>();

    for (OCluster cluster : clusterInstances) {
      try {
        Integer exits = names.get(cluster.getName());
        if (exits != null) {
          newAllocators[cluster.getId()] = allocators[exits];
        } else {
          long next = cluster.getLastPosition() + 1;
          newAllocators[cluster.getId()] = new AtomicLong(next);
        }
        newNames.put(cluster.getName(), cluster.getId());
      } catch (IOException e) {
        OLogManager.instance().error(this, "error resolving last position", e);
      }
    }
    allocators = newAllocators;
    names = newNames;

  }
}
