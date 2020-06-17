package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.distributed.impl.coordinator.OClusterPositionAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class OClusterPositionAllocatorDatabase implements OClusterPositionAllocator {

  private volatile Map<String, Integer> names = new HashMap<>();
  private volatile List<AtomicLong> allocators;
  private volatile OSharedContext sharedContext;

  public OClusterPositionAllocatorDatabase(OSharedContext context) {
    this.sharedContext = context;
    registerListener(context);
  }

  private void registerListener(OSharedContext sharedContext) {

    sharedContext.registerListener(
        new OMetadataUpdateListener() {
          @Override
          public void onSchemaUpdate(String database, OSchemaShared schema) {
            initAllocators(sharedContext.getStorage());
          }

          @Override
          public void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager) {}

          @Override
          public void onFunctionLibraryUpdate(String database) {}

          @Override
          public void onSequenceLibraryUpdate(String database) {}

          @Override
          public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
            // this may not be perfect now because allocation may have happened in parallel to
            // schema operation now that the schema operations don't go through the coordinator.
            initAllocators(sharedContext.getStorage());
          }
        });

    initAllocators(sharedContext.getStorage());
  }

  @Override
  public synchronized long allocate(int clusterId) {
    // Syncrhonized will be replaced when the schema operations will go through the coordinator
    return allocators.get(clusterId).getAndIncrement();
  }

  private synchronized void initAllocators(OStorage storage) {
    // Syncrhonized will be replaced when the schema operations will go through the coordinator
    int clusters = storage.getClusters();
    Collection<? extends OCluster> clusterInstances = storage.getClusterInstances();
    ArrayList<AtomicLong> newAllocators = new ArrayList<>(clusters);
    Map<String, Integer> newNames = new HashMap<>();

    for (OCluster cluster : clusterInstances) {
      try {
        Integer exits = names.get(cluster.getName());
        if (newAllocators.size() <= cluster.getId()) {
          for (int i = cluster.getId() - newAllocators.size(); i >= 0; i--) {
            newAllocators.add(null);
          }
        }
        if (exits != null) {
          newAllocators.set(cluster.getId(), allocators.get(exits));
        } else {
          long next = cluster.getLastPosition() + 1;
          newAllocators.set(cluster.getId(), new AtomicLong(next));
        }
        newNames.put(cluster.getName(), cluster.getId());
      } catch (IOException e) {
        OLogManager.instance().error(this, "error resolving last position", e);
      }
    }
    allocators = newAllocators;
    names = newNames;
  }

  public void reload() {
    initAllocators(sharedContext.getStorage());
  }
}
