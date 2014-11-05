package com.orientechnologies.orient.test.database.speed;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
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

    ORecordIteratorClass iterator = new ORecordIteratorClass(db, (ODatabaseRecordTx) db.getUnderlying(), "SpeedTest", true);
    iterator.setRange(new ORecordId(oClass.getDefaultClusterId(), 999998), new ORecordId(oClass.getDefaultClusterId(), 999999));

    long start = System.nanoTime();

    while (iterator.hasNext())
      iterator.next();

    long end = System.nanoTime();
    System.out.println(end - start);

    db.drop();
  }
}
