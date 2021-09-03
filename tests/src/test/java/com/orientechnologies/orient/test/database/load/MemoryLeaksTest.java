package com.orientechnologies.orient.test.database.load;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

/**
 * Check the system as for memory leaks.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class MemoryLeaksTest {

  @Test
  public void createLotsOfRecordsWithBigContent() {
    ODatabaseDocumentTx vDb = new ODatabaseDocumentTx("plocal:target/MemoryLeaksTest");
    vDb.create();
    for (int i = 0; i < 10000000; i++) {
      ODocument vDoc = new ODocument();
      vDoc.field("test", new byte[100000]);
      vDoc.save();
      if (i % 10 == 0)
        System.out.println("Records created:" + i + " cacheSize: " + vDb.getLocalCache().getSize());
    }
  }
}
