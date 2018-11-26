package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;

class SchemaSnapshotOnIndexesUpdateListener implements OMetadataUpdateListener {
  private OSchemaShared schema;

  public SchemaSnapshotOnIndexesUpdateListener(OSchemaShared schema) {
    this.schema = schema;
  }

  @Override
  public void onSchemaUpdate(String database, OSchemaShared schema) {

  }

  @Override
  public void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager) {
    schema.forceSnapshot(ODatabaseRecordThreadLocal.instance().get());
  }

  @Override
  public void onFunctionLibraryUpdate(String database) {

  }

  @Override
  public void onSequenceLibraryUpdate(String database) {

  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {

  }
}
