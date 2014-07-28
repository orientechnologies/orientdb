package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.*;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordMetadata;

/**
 * @author edegtyarenko
 * @since 11.03.13 12:00
 */
@Test(groups = { "crud" })
public class RecordMetadataTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public RecordMetadataTest(@Optional String url) {
    super(url);
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
