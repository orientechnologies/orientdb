package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OStorageTestIT {
  private OrientDB orientDB;

  private static Path buildPath;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildPath = Paths.get(buildDirectory);
    Files.createDirectories(buildPath);
  }

  @Test
  public void testCheckSumFailureReadOnly() throws Exception {

    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
    orientDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession session =
        orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = ((ODatabaseDocumentInternal) session).getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.close(true, false);
    ctx.close();

    int position = 3 * 1024;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);

    int bt = file.read();
    file.seek(position);
    file.write(bt + 1);
    file.close();

    session = orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (OStorageException e) {
      orientDB.close();
      orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
      orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberReadOnly() throws Exception {
    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
    orientDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession session =
        orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = ((ODatabaseDocumentInternal) session).getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.close(true, false);
    ctx.close();

    int position = OFile.HEADER_SIZE + ODurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    session = orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (OStorageException e) {
      orientDB.close();
      orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
      orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberVerify() throws Exception {

    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
    orientDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession session =
        orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = ((ODatabaseDocumentInternal) session).getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.close(true, false);
    ctx.close();

    int position = OFile.HEADER_SIZE + ODurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    session = orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
    document.field("value", "value");

    document.save();

    session.close();
  }

  @Test
  public void testCheckSumFailureVerifyAndLog() throws Exception {

    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), config);
    orientDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession session =
        orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = ((ODatabaseDocumentInternal) session).getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.close(true, false);
    ctx.close();

    int position = 3 * 1024;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);

    int bt = file.read();
    file.seek(position);
    file.write(bt + 1);
    file.close();

    session = orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
    document.field("value", "value");

    document.save();

    session.close();
  }

  @Test
  public void testCreatedVersionIsStored() {
    orientDB =
        new OrientDB(
            "embedded:" + buildPath.toFile().getAbsolutePath(), OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    final ODatabaseSession session =
        orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try (OResultSet resultSet = session.query("SELECT FROM metadata:storage")) {
      Assert.assertTrue(resultSet.hasNext());

      final OResult result = resultSet.next();
      Assert.assertEquals(OConstants.getVersion(), result.getProperty("createdAtVersion"));
    }
  }

  @After
  public void after() {
    orientDB.drop(OStorageTestIT.class.getSimpleName());
  }
}
