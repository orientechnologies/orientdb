package com.orientechnologies.test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by enricorisa on 08/04/14.
 */
public class LuceneNotUniqueTest {

  @Test
  public void testFindRome() {
    ODatabaseDocumentTx databaseDocumentTx = getDb();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass city = schema.getClass("City");

    OIndex index = city.getClassIndex("City.name");
    if (index == null)
      city.createIndex("City.name", "NOTUNIQUE", null, null, "LUCENE", new String[] { "name" });

    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<Object>("SELECT from City where name = 'Rome'"));

    Assert.assertEquals(docs.size(), 152);

    docs = databaseDocumentTx.query(new OSQLSynchQuery<Object>("SELECT from City where name = 'London'"));

    Assert.assertEquals(docs.size(), 1049);

  }

  private ODatabaseDocumentTx getDb() {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/location");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
    }
    return databaseDocumentTx;
  }
}
