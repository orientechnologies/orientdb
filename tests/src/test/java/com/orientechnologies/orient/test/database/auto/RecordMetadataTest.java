package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordMetadata;

/**
 * @author edegtyarenko
 * @since 11.03.13 12:00
 */
@Test(groups = { "crud" })
public class RecordMetadataTest {

  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public RecordMetadataTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeMethod
  public void open() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void close() {
    database.close();
  }

  public void testGetRecordMetadata() {

    final ODocument doc = new ODocument();

    for (int i = 0; i < 5; i++) {
      doc.field("field", i);
      database.save(doc);

      final ORecordMetadata metadata = database.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(doc.getRecordVersion().getCounter(), metadata.getRecordVersion().getCounter());
    }
  }

  private static void assetORIDEquals(ORID actual, ORID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition().longValueHigh(), expected.getClusterPosition().longValueHigh());
  }
}
