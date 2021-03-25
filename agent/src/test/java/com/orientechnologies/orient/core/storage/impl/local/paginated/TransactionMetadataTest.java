package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.junit.Assert.*;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.junit.*;

public class TransactionMetadataTest {

  private OrientDB orientDB;
  private ODatabaseSession db;
  private static final String DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {

    orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database `" + DB_NAME + "` plocal users(admin identified by 'admin' role admin)");
    db = orientDB.open(DB_NAME, "admin", "admin");
  }

  @Test
  public void testBackupRestore() throws IOException {
    db.begin();
    byte[] metadata = new byte[] {1, 2, 4};
    ((OTransactionInternal) db.getTransaction())
        .setMetadataHolder(Optional.of(new TestMetadataHolder(metadata)));
    OVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    db.incrementalBackup("target/backup_metadata");
    db.close();
    OrientDBInternal.extract(orientDB)
        .restore(
            DB_NAME + "_re",
            null,
            null,
            ODatabaseType.PLOCAL,
            "target/backup_metadata",
            OrientDBConfig.defaultConfig());
    ODatabaseSession db1 = orientDB.open(DB_NAME + "_re", "admin", "admin");
    Optional<byte[]> fromStorage =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db1).getStorage())
            .getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @After
  public void after() {
    OFileUtils.deleteRecursively(new File("target/backup_metadata"));
    db.close();
    orientDB.drop(DB_NAME);
    if (orientDB.exists(DB_NAME + "_re")) {
      orientDB.drop(DB_NAME + "_re");
    }
    orientDB.close();
  }

  private static class TestMetadataHolder implements OTxMetadataHolder {
    private final byte[] metadata;

    public TestMetadataHolder(byte[] metadata) {
      this.metadata = metadata;
    }

    @Override
    public byte[] metadata() {
      return metadata;
    }

    @Override
    public void notifyMetadataRead() {}

    @Override
    public OTransactionId getId() {
      return null;
    }

    @Override
    public OTransactionSequenceStatus getStatus() {
      return null;
    }
  }
}
