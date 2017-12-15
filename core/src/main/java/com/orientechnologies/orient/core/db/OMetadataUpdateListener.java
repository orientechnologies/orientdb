package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;

public interface OMetadataUpdateListener {
  void onSchemaUpdate(OSchema schema);

  void onIndexManagerUpdate(OIndexManager indexManager);

  void onFunctionLibraryUpdate(OFunctionLibrary oFunctionLibrary);

  void onSequenceLibraryUpdate(OSequenceLibraryImpl oSequenceLibrary);

  void onStorageConfigurationUpdate(OStorageConfiguration update);
}
