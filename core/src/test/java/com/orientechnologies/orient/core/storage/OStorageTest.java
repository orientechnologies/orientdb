package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OPageIsBrokenException;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import org.junit.BeforeClass;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

@Test
public class OStorageTest {
  private File                buildPath;
  private ODatabaseDocumentTx database;

  @BeforeClass
  public void beforeClass() throws IOException {
    OGlobalConfiguration.STORAGE_CHECKSUM_MODE.setValue(OChecksumMode.StoreAndSwitchReadOnlyMode);
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildPath = new File(buildDirectory);

    if (!buildPath.exists())
      Assert.assertTrue(buildPath.mkdirs());
  }

  @Test
  public void testCheckSumFailureReadOnly() throws Exception {
    OFileUtils.deleteRecursively(new File(buildPath, OStorageTest.class.getSimpleName()));

    database = new ODatabaseDocumentTx("plocal:" + new File(buildPath, OStorageTest.class.getSimpleName()).getCanonicalPath());
    database.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1);

    database.create();

    OMetadata metadata = database.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage = (OLocalPaginatedStorage) database.getStorage();
    database.close();

    final String storagePath = storage.getStoragePath();

    storage.close(true, false);

    int position = 3 * 1024;

    File fileToBreak = new File(storagePath, "pagebreak.pcl");
    Assert.assertTrue(fileToBreak.exists());

    RandomAccessFile file = new RandomAccessFile(fileToBreak, "rw");
    file.seek(position);

    int bt = file.read();
    file.seek(position);
    file.write(bt + 1);
    file.close();

    database = new ODatabaseDocumentTx("plocal:" + new File(buildPath, OStorageTest.class.getSimpleName()).getCanonicalPath());
    database.open("admin", "admin");
    database.query(new OSQLSynchQuery<Object>("select from PageBreak"));

    Thread.sleep(100);//lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
    document.field("value", "value");

    try {
      document.save();
      Assert.fail();
    } catch (OPageIsBrokenException e) {
      Assert.assertTrue(true);
    }

    database.close();
  }

  @Test
  public void testCheckMagicNumberReadOnly() throws Exception {
    OFileUtils.deleteRecursively(new File(buildPath, OStorageTest.class.getSimpleName()));

    database = new ODatabaseDocumentTx("plocal:" + new File(buildPath, OStorageTest.class.getSimpleName()).getCanonicalPath());
    database.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1);

    database.create();

    OMetadata metadata = database.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage = (OLocalPaginatedStorage) database.getStorage();
    database.close();

    final String storagePath = storage.getStoragePath();
    storage.close(true, false);

    int position = OFileClassic.HEADER_SIZE + ODurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file = new RandomAccessFile(new File(storagePath, "pagebreak.pcl"), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    database = new ODatabaseDocumentTx("plocal:" + new File(buildPath, OStorageTest.class.getSimpleName()).getCanonicalPath());
    database.open("admin", "admin");
    database.query(new OSQLSynchQuery<Object>("select from PageBreak"));

    Thread.sleep(100);//lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
    document.field("value", "value");

    try {
      document.save();
      Assert.fail();
    } catch (OPageIsBrokenException e) {
      Assert.assertTrue(true);
    }

    database.close();
  }

  @AfterMethod
  public void after() {
    if (database != null) {
      database.open("admin", "admin");
      database.drop();
    }
  }

}
