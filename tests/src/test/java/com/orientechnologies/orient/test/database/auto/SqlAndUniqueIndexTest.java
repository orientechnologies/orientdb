package com.orientechnologies.orient.test.database.auto;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.server.OServer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class SqlAndUniqueIndexTest {

  private static final String CLASS_NAME = "ClassTest";

  private static final String[] PROPERTIES = { "prop_1", "prop_2", "prop_3" };

  private static final String INDEX_NAME = "UniqueIndex";

  private String url;

  private OServer server;

  @BeforeClass
  public void start() throws Exception {

    File file = File.createTempFile("orientdb-test-", "");
    file.delete();
    file.mkdirs();
    file.deleteOnExit();

    url = "local:" + file.getAbsolutePath();

    System.setProperty("ORIENTDB_HOME", url);
    server = new OServer().startup().activate();

    // create database if does not exist
    ODatabase database = new OObjectDatabaseTx(url);
    if(!database.exists()) database.create();
    database.close();
  }

  @AfterClass
  public void stop() {
    if(server != null) server.shutdown();
  }

  @Test
  public void testIndexAndSQL() {

    createUniqueIndex();

    Collection<Object> values = new ArrayList<Object>();
    Map<String, Object> fields = new HashMap<String, Object>();
    for(String property : PROPERTIES) {
      String value = "value for " + property;
      fields.put(property, value);
      values.add(value);
    }
    save(fields);

    // find from unique index
    ODocument docFromIndex = findUniqueFromIndex(values.toArray(new Object[values.size()]));
    assertNotNull(docFromIndex);
    assertEquals(docFromIndex.getClassName(), CLASS_NAME);
    for(String property : PROPERTIES) {
      assertEquals(docFromIndex.field(property), "value for " + property);
    }

    // find all
    ORecordIteratorClass<ODocument> all = findAll();
    Iterator<ODocument> iterator = all.iterator();
    assertTrue(iterator.hasNext());
    ODocument docFromAll = iterator.next();
    assertNotNull(docFromAll);
    assertEquals(docFromAll.getClassName(), CLASS_NAME);
    for(String property : PROPERTIES) {
      assertEquals(docFromAll.field(property), "value for " + property);
    }
    assertFalse(iterator.hasNext());

    // find using SQL query
    List<ODocument> docsFromSql = findWithSql(values);
    assertNotNull(docsFromSql);
    assertEquals(docsFromSql.size(), 1);
    ODocument docFromSql = docsFromSql.get(0);
    assertEquals(docFromSql.getClassName(), CLASS_NAME);
    for(String property : PROPERTIES) {
      assertEquals(docFromSql.field(property), "value for " + property);
    }
  }

  private void createUniqueIndex() {

    ODatabaseDocumentTx db = getDocumentTx();
    try {

      OClass indexClass;
      OSchema schema = db.getMetadata().getSchema();
      if(schema.existsClass(CLASS_NAME)) {
        indexClass = schema.getClass(CLASS_NAME);
      } else {
        indexClass = schema.createClass(CLASS_NAME);
        schema.save();
      }

      for(String propertyPath : PROPERTIES) {
        OProperty property = indexClass.getProperty(propertyPath);
        if(property == null) {
          indexClass.createProperty(propertyPath, OType.STRING);
          schema.save();
        }
      }

      indexClass.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE, PROPERTIES);

    } finally {
      db.close();
    }
  }

  private void save(Map<String, Object> fields) {
    ODatabaseDocumentTx db = getDocumentTx();
    try {
      ODocument document = new ODocument(CLASS_NAME);
      document.fields(fields);
      db.begin(OTransaction.TXTYPE.OPTIMISTIC);
      document.save();
      db.commit();
    } catch(OException e) {
      db.rollback();
      throw e;
    } finally {
      db.close();
    }
  }

  private ODocument findUniqueFromIndex(Object... values) {
    ODatabaseDocumentTx db = getDocumentTx();
    try {
      OIndex<?> index = db.getMetadata().getIndexManager().getIndex(INDEX_NAME);
      Object key = new OCompositeKey(values);
      OIdentifiable identifiable = (OIdentifiable) index.get(key);
      return identifiable == null ? null : identifiable.<ODocument>getRecord();
    } finally {
      db.close();
    }
  }

  private List<ODocument> findWithSql(Object... values) {
    ODatabaseDocumentTx db = getDocumentTx();
    try {
      return db.query(new OSQLSynchQuery<ODocument>(
          "select from " + CLASS_NAME + " where prop_1 = ? and prop_2 = ? and prop_3 = ?"), values);
    } finally {
      db.close();
    }
  }

  private ORecordIteratorClass<ODocument> findAll() {
    ODatabaseDocumentTx db = getDocumentTx();
    try {
      return db.browseClass(CLASS_NAME);
    } finally {
      db.close();
    }
  }

  private ODatabaseDocumentTx getDocumentTx() {
    return ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
  }

}
