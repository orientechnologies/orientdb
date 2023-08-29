package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class OChainIndexFetchTest extends BaseMemoryDatabase {

  @Test
  public void testFetchChaninedIndex() {
    OClass baseClass = db.getMetadata().getSchema().createClass("BaseClass");
    OProperty propr = baseClass.createProperty("ref", OType.LINK);

    OClass linkedClass = db.getMetadata().getSchema().createClass("LinkedClass");
    OProperty id = linkedClass.createProperty("id", OType.STRING);
    id.createIndex(INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(linkedClass);
    propr.createIndex(INDEX_TYPE.NOTUNIQUE);

    ODocument doc = new ODocument(linkedClass);
    doc.field("id", "referred");
    db.save(doc);

    ODocument doc1 = new ODocument(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);

    OResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
