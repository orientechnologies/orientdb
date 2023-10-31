package com.orientechnologies.orient.core.index;

import org.junit.Test;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class BigKeyIndexTest extends BaseMemoryDatabase {

  @Test
  public void testBigKey() {
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      ODocument doc = db.newInstance("One");
      String bigValue = i % 1000 + "one10000";
      for (int z = 0; z < 1000; z++) {
        bigValue += "one" + z;
      }
      doc.setProperty("two", bigValue);
      db.save(doc);
    }
  }

  @Test(expected = OTooBigIndexKeyException.class)
  public void testTooBigKey() {
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    ODocument doc = db.newInstance("One");
    String bigValue = "";
    for (int z = 0; z < 3000; z++) {
      bigValue += "one" + z;
    }
    doc.setProperty("two", bigValue);
    db.save(doc);
  }
}
