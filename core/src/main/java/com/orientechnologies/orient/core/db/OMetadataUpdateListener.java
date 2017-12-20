package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;

public interface OMetadataUpdateListener {
  void onSchemaUpdate(String database, OSchemaShared schema);

  void onIndexManagerUpdate(String database, OIndexManager indexManager);

  void onFunctionLibraryUpdate(String database, OFunctionLibrary oFunctionLibrary);

  void onSequenceLibraryUpdate(String database, OSequenceLibraryImpl oSequenceLibrary);

  void onStorageConfigurationUpdate(String database, OStorageConfiguration update);
}
