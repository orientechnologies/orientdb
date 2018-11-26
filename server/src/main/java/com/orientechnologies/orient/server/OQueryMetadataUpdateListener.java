package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

class OQueryMetadataUpdateListener implements OMetadataUpdateListener {
  private boolean updated = false;

  @Override
  public void onSchemaUpdate(String database, OSchemaShared schema) {
    updated = true;
  }

  @Override
  public void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager) {
    updated = true;
  }

  @Override
  public void onFunctionLibraryUpdate(String database) {
    updated = true;
  }

  @Override
  public void onSequenceLibraryUpdate(String database) {
    updated = true;
  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
    updated = true;
  }

  public boolean isUpdated() {
    return updated;
  }
}
