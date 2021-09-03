package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/17/14
 */
@Test
public class IteratorSpeedTest {
  public void testIterationSpeed() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:speedTest");
    db.create();

    OClass oClass = db.getMetadata().getSchema().createClass("SpeedTest");
    for (int i = 0; i < 1000000; i++) {
      ODocument document = new ODocument("SpeedTest");
      document.save();
    }

    ORecordIteratorClass iterator = new ORecordIteratorClass(db, "SpeedTest", true);
    iterator.setRange(
        new ORecordId(oClass.getDefaultClusterId(), 999998),
        new ORecordId(oClass.getDefaultClusterId(), 999999));

    long start = System.nanoTime();

    while (iterator.hasNext()) iterator.next();

    long end = System.nanoTime();
    System.out.println(end - start);

    db.drop();
  }
}
