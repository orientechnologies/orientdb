package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;

public interface OMetadataUpdateListener {

  void onSchemaUpdate(String database, OSchemaShared schema);

  void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager);

  void onFunctionLibraryUpdate(String database);

  void onSequenceLibraryUpdate(String database);

  void onStorageConfigurationUpdate(String database, OStorageConfiguration update);
}
