package com.orientechnologies.orient.core.storage.index.sbtree.local.v1;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.File;

public class SBTreeV1TestEncryptionTestIT extends SBTreeV1TestIT {
  @Override
  public void before() throws Exception {
    buildDirectory = System.getProperty("buildDirectory", ".");

    dbName = "localSBTreeEncryptedTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    orientDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");
    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    sbTree =
        new OSBTreeV1<>(
            "sbTreeEncrypted",
            ".sbt",
            ".nbt",
            (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage());
    storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseDocumentTx).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    final OEncryption encryption =
        OEncryptionFactory.INSTANCE.getEncryption("aes/gcm", "T1JJRU5UREJfSVNfQ09PTA==");
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(
                atomicOperation,
                OIntegerSerializer.INSTANCE,
                OLinkSerializer.INSTANCE,
                null,
                1,
                false,
                encryption));
  }
}
