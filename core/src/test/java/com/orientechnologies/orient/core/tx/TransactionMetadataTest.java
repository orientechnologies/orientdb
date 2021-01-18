package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionMetadataTest {

  private OrientDB orientDB;
  private ODatabaseSession db;
  private static final String DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            DB_NAME, "embedded:./target/", OCreateDatabaseUtil.TYPE_PLOCAL);
    db = orientDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    db.begin();
    byte[] metadata = new byte[] {1, 2, 4};
    ((OTransactionInternal) db.getTransaction())
        .setMetadataHolder(Optional.of(new TestMetadataHolder(metadata)));
    OVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    db.close();
    orientDB.close();

    orientDB =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db = orientDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    Optional<byte[]> fromStorage =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage())
            .getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
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
    db.backup(out, null, null, null, 1, 1024);
    db.close();

    orientDB.execute(
        "create database "
            + DB_NAME
            + "_re"
            + " "
            + "plocal"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    ODatabaseSession db1 =
        orientDB.open(DB_NAME + "_re", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    db1.restore(new ByteArrayInputStream(out.toByteArray()), null, null, null);
    db1.close();
    db1 = orientDB.open(DB_NAME + "_re", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    Optional<byte[]> fromStorage =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db1).getStorage())
            .getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @After
  public void after() {
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
