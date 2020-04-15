package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.*;

public class TransactionMetadataTest {

  private              OrientDB         orientDB;
  private              ODatabaseSession db;
  private static final String           DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {

    orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);
    db = orientDB.open(DB_NAME, "admin", "admin");
  }

  @Test
  public void test() {
    db.begin();
    byte[] metadata = new byte[] { 1, 2, 4 };
    ((OTransactionInternal) db.getTransaction()).setMetadataHolder(Optional.of(new TestMetadataHolder(metadata)));
    OVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    db.close();
    orientDB.close();
    orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    db = orientDB.open(DB_NAME, "admin", "admin");
    Optional<byte[]> fromStorage = ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage()).getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @Test
  public void testBackupRestore() throws IOException {
    db.begin();
    byte[] metadata = new byte[] { 1, 2, 4 };
    ((OTransactionInternal) db.getTransaction()).setMetadataHolder(Optional.of(new TestMetadataHolder(metadata)));
    OVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    db.backup(out, null, null, null, 1, 1024);
    db.close();
    orientDB.create(DB_NAME + "_re", ODatabaseType.PLOCAL);
    ODatabaseSession db1 = orientDB.open(DB_NAME + "_re", "admin", "admin");
    db1.restore(new ByteArrayInputStream(out.toByteArray()), null, null, null);
    db1.close();
    db1 = orientDB.open(DB_NAME + "_re", "admin", "admin");
    Optional<byte[]> fromStorage = ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db1).getStorage()).getLastMetadata();
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
    public void notifyMetadataRead() {

    }

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
