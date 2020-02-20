package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.*;

import java.util.Optional;

import static org.junit.Assert.*;

public class TransactionMetadataTest {

  private              OrientDB         orientDB;
  private              ODatabaseSession db;
  private static final String           DB_NAME = TransactionMetadataTest.class.

      getSimpleName();

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
    ((OTransactionInternal) db.getTransaction()).setMetadata(Optional.of(metadata));
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
    assertArrayEquals(fromStorage.get(),metadata);
  }

  @After
  public void after() {
    db.close();
    orientDB.drop(DB_NAME);
    orientDB.close();
  }

}
