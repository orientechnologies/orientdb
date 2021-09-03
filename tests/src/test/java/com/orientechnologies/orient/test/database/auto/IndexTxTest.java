package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class IndexTxTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexTxTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.createClass("IndexTxTestClass");
    cls.createProperty("name", OType.STRING);
    cls.createIndex("IndexTxTestIndex", OClass.INDEX_TYPE.UNIQUE, "name");
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.getClass("IndexTxTestClass");
    if (cls != null) {
      cls.truncate();
    }
  }

  @Test
  public void testIndexCrossReferencedDocuments() {
    checkEmbeddedDB();

    database.begin();

    final ODocument doc1 = new ODocument("IndexTxTestClass");
    final ODocument doc2 = new ODocument("IndexTxTestClass");

    doc1.save();
    doc2.save();

    doc1.field("ref", doc2.getIdentity().copy());
    doc1.field("name", "doc1");
    doc2.field("ref", doc1.getIdentity().copy());
    doc2.field("name", "doc2");

    doc1.save();
    doc2.save();

    database.commit();

    Map<String, ORID> expectedResult = new HashMap<>();
    expectedResult.put("doc1", doc1.getIdentity());
    expectedResult.put("doc2", doc2.getIdentity());

    OIndex index = getIndex("IndexTxTestIndex");
    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();

        final ORID expectedValue = expectedResult.get(key);
        final ORID value;
        try (Stream<ORID> stream = index.getInternal().getRids(key)) {
          value = stream.findAny().orElse(null);
        }

        Assert.assertNotNull(value);
        Assert.assertTrue(value.isPersistent());
        Assert.assertEquals(value, expectedValue);
      }
    }
  }
}
