package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;

public class LocalHashTableRollbackEncryptionIT extends LocalHashTableRollbackIT {
  @Override
  protected OrientDBConfig getConfig() {
    return new OrientDBConfigBuilder().addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, "aes/gcm").
        addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==").build();
  }
}
