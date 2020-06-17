package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author edegtyarenko
 * @since 11.03.13 12:00
 */
@Test(groups = {"crud"})
public class RecordMetadataTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public RecordMetadataTest(@Optional String url) {
    super(url);
  }

  private static void assetORIDEquals(ORID actual, ORID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition(), expected.getClusterPosition());
  }

  public void testGetRecordMetadata() {

    final ODocument doc = new ODocument();

    for (int i = 0; i < 5; i++) {
      doc.field("field", i);
      database.save(doc, database.getClusterNameById(database.getDefaultClusterId()));

      final ORecordMetadata metadata = database.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(doc.getVersion(), metadata.getVersion());
    }
  }
}
