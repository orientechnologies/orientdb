package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(String database, OSchemaShared schema);

  void onIndexManagerUpdate(String database, OIndexManager indexManager);

  void onFunctionLibraryUpdate(String database);

  void onSequenceLibraryUpdate(String database);

  void onStorageConfigurationUpdate(String database, OStorageConfiguration update);
}
