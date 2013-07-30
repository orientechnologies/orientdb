package com.orientechnologies.orient.core.sql;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class TestOrderByIndexPropDesc {

  private static final String DOCUMENT_CLASS_NAME = "MyDocument";
  private static final String PROP_INDEXED_STRING = "dateProperty";

  private ODatabaseDocument   db;

  @BeforeMethod
  public void init() throws Exception {
    db = new ODatabaseDocumentTx("memory:test");
    db.create();
    OClass oclass = db.getMetadata().getSchema().createClass(DOCUMENT_CLASS_NAME);
    oclass.createProperty(PROP_INDEXED_STRING, OType.INTEGER);
    oclass.createIndex("index", INDEX_TYPE.NOTUNIQUE, PROP_INDEXED_STRING);
  }

  @AfterMethod
  public void drop() {
    if (db != null) {
      db.drop();
    }
  }

  @Test
  public void worksFor1000() {
    test(1000);
  }

  @Test
  public void worksFor10000() {
    test(50000);
  }

  private void test(int count) {
    ODocument doc = db.newInstance();
    for (int i = 0; i < count; i++) {
      doc.reset();
      doc.setClassName(DOCUMENT_CLASS_NAME);
      doc.field(PROP_INDEXED_STRING, i);
      db.save(doc);
    }

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from " + DOCUMENT_CLASS_NAME + " order by "
        + PROP_INDEXED_STRING + " desc"));
    for (ODocument d : result) {
      // System.out.println(d.<Integer>field(PROP_INDEXED_STRING));
    }

    assertEquals(count, result.size());

  }

}
