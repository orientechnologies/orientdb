/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
public class CRUDDocumentPhysicalTest {
  protected static final int  TOT_RECORDS         = 100;
  protected static final int  TOT_RECORDS_COMPANY = 10;

  protected long              startRecordNumber;
  private ODatabaseDocumentTx database;
  private ODocument           record;
  private String              url;
  String                      base64;

  @Parameters(value = "url")
  public CRUDDocumentPhysicalTest(final String iURL) {
    url = iURL;
  }

  @Test
  public void testPool() throws IOException {
    final ODatabaseDocumentTx[] dbs = new ODatabaseDocumentTx[ODatabaseDocumentPool.global().getMaxSize()];

    for (int i = 0; i < 10; ++i) {
      for (int db = 0; db < dbs.length; ++db)
        dbs[db] = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
      for (int db = 0; db < dbs.length; ++db)
        dbs[db].close();
    }
  }

  @Test
  public void cleanAll() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      record = database.newInstance();

      startRecordNumber = database.countClusterElements("Account");

      // DELETE ALL THE RECORDS IN THE CLUSTER
      while (database.countClusterElements("Account") > 0)
        for (ODocument rec : database.browseCluster("Account"))
          if (rec != null)
            rec.delete();

      Assert.assertEquals(database.countClusterElements("Account"), 0);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "cleanAll")
  public void create() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      startRecordNumber = database.countClusterElements("Account");

      byte[] binary = new byte[100];
      for (int b = 0; b < binary.length; ++b)
        binary[b] = (byte) b;

      base64 = OBase64Utils.encodeBytes(binary);

      final int accountClusterId = database.getClusterIdByName("Account");

      for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
        record.reset();

        record.setClassName("Account");
        record.field("id", i);
        record.field("name", "Gipsy");
        record.field("location", "Italy");
        record.field("salary", (i + 300f));
        record.field("binary", binary);
        record.field("nonSchemaBinary", binary);
        record.field("testLong", 10000000000L); // TEST LONG
        record.field("extra", "This is an extra field not included in the schema");
        record.field("value", (byte) 10);

        record.save();
        Assert.assertEquals(record.getIdentity().getClusterId(), accountClusterId);
      }

      long startRecordNumberL = database.countClusterElements("Company");
      final ODocument doc = new ODocument();
      for (long i = startRecordNumberL; i < startRecordNumberL + TOT_RECORDS_COMPANY; ++i) {
        doc.setClassName("Company");
        doc.field("id", i);
        doc.field("name", "Microsoft" + i);
        doc.field("employees", (int) (100000 + i));
        database.save(doc);
        doc.reset();
      }
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testCreate")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      // BROWSE IN THE OPPOSITE ORDER
      byte[] binary;

      Set<Integer> ids = new HashSet<Integer>();

      for (int i = 0; i < TOT_RECORDS; i++)
        ids.add(i);

      ORecordIteratorCluster<ODocument> it = database.browseCluster("Account");
      for (it.last(); it.hasPrevious();) {
        ODocument rec = it.previous();

        if (rec != null) {
          int id = ((Number) rec.field("id")).intValue();
          Assert.assertTrue(ids.remove(id));
          Assert.assertEquals(rec.field("name"), "Gipsy");
          Assert.assertEquals(rec.field("location"), "Italy");
          Assert.assertEquals(((Number) rec.field("testLong")).longValue(), 10000000000L);
          Assert.assertEquals(((Number) rec.field("salary")).intValue(), id + 300);
          Assert.assertNotNull(rec.field("extra"));
          Assert.assertEquals(((Byte) rec.field("value", Byte.class)).byteValue(), (byte) 10);

          binary = rec.field("binary", OType.BINARY);

          for (int b = 0; b < binary.length; ++b)
            Assert.assertEquals(binary[b], (byte) b);
        }
      }

      Assert.assertTrue(ids.isEmpty());

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void update() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      int i = 0;
      for (ODocument rec : database.browseCluster("Account")) {

        if (i % 2 == 0)
          rec.field("location", "Spain");

        rec.field("price", i + 100);

        rec.save();

        i++;
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      for (ODocument rec : database.browseCluster("Account")) {
        int price = ((Number) rec.field("price")).intValue();
        Assert.assertTrue(price - 100 >= 0);

        if ((price - 100) % 2 == 0)
          Assert.assertEquals(rec.field("location"), "Spain");
        else
          Assert.assertEquals(rec.field("location"), "Italy");
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testDoubleChanges() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      ODocument vDoc = database.newInstance();
      vDoc.setClassName("Profile");
      vDoc.field("nick", "JayM1").field("name", "Jay").field("surname", "Miner");
      vDoc.save();

      Assert.assertEquals(vDoc.getIdentity().getClusterId(), vDoc.getSchemaClass().getDefaultClusterId());

      vDoc = database.load(vDoc.getIdentity());
      vDoc.field("nick", "JayM2");
      vDoc.field("nick", "JayM3");
      vDoc.save();

      Set<OIndex<?>> indexes = database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getIndexes();

      Assert.assertEquals(indexes.size(), 1);

      OIndex indexDefinition = indexes.iterator().next();
      OIdentifiable vOldName = (OIdentifiable) indexDefinition.get("JayM1");
      Assert.assertNull(vOldName);

      OIdentifiable vIntermediateName = (OIdentifiable) indexDefinition.get("JayM2");
      Assert.assertNull(vIntermediateName);

      OIdentifiable vNewName = (OIdentifiable) indexDefinition.get("JayM3");
      Assert.assertNotNull(vNewName);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testDoubleChanges")
  public void testMultiValues() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      ODocument vDoc = database.newInstance();
      vDoc.setClassName("Profile");
      vDoc.field("nick", "Jacky").field("name", "Jack").field("surname", "Tramiel");
      vDoc.save();

      // add a new record with the same name "nameA".
      vDoc = database.newInstance();
      vDoc.setClassName("Profile");
      vDoc.field("nick", "Jack").field("name", "Jack").field("surname", "Bauer");
      vDoc.save();

      Collection<OIndex<?>> indexes = database.getMetadata().getSchema().getClass("Profile").getProperty("name").getIndexes();
      Assert.assertEquals(indexes.size(), 1);

      OIndex<?> indexName = indexes.iterator().next();
      // We must get 2 records for "nameA".
      Collection<OIdentifiable> vName1 = (Collection<OIdentifiable>) indexName.get("Jack");
      Assert.assertEquals(vName1.size(), 2);

      // Remove this last record.
      database.delete(vDoc);

      // We must get 1 record for "nameA".
      vName1 = (Collection<OIdentifiable>) indexName.get("Jack");
      Assert.assertEquals(vName1.size(), 1);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testMultiValues")
  public void testUnderscoreField() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      ODocument vDoc = database.newInstance();
      vDoc.setClassName("Profile");
      vDoc.field("nick", "MostFamousJack").field("name", "Kiefer").field("surname", "Sutherland")
          .field("tag_list", new String[] { "actor", "myth" });
      vDoc.save();

      List<ODocument> result = database.command(
          new OSQLSynchQuery<ODocument>("select from Profile where name = 'Kiefer' and tag_list.size() > 0 ")).execute();

      Assert.assertEquals(result.size(), 1);
    } finally {
      database.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDbCacheUpdated() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      ODocument vDoc = database.newInstance();
      vDoc.setClassName("Profile");

      Set<String> tags = new HashSet<String>();
      tags.add("test");
      tags.add("yeah");

      vDoc.field("nick", "Dexter").field("name", "Michael").field("surname", "Hall").field("tag_list", tags);
      vDoc.save();

      List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where name = 'Michael'"))
          .execute();

      Assert.assertEquals(result.size(), 1);
      ODocument dexter = result.get(0);
      ((Collection<String>) dexter.field("tag_list")).add("actor");

      dexter.setDirty();
      dexter.save();

      result = database.command(
          new OSQLSynchQuery<ODocument>("select from Profile where tag_list in 'actor' and tag_list in 'test'")).execute();
      Assert.assertEquals(result.size(), 1);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testUnderscoreField")
  public void testUpdateLazyDirtyPropagation() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      for (ODocument rec : database.browseCluster("Profile")) {
        Assert.assertFalse(rec.isDirty());

        Collection<?> followers = rec.field("followers");
        if (followers != null && followers.size() > 0) {
          followers.remove(followers.iterator().next());
          Assert.assertTrue(rec.isDirty());
          break;
        }
      }

    } finally {
      database.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNestedEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, HashMap<?, ?>> map1 = new HashMap<String, HashMap<?, ?>>();
    newDoc.field("map1", map1, OType.EMBEDDEDMAP);

    final Map<String, HashMap<?, ?>> map2 = new HashMap<String, HashMap<?, ?>>();
    map1.put("map2", (HashMap<?, ?>) map2);

    final Map<String, HashMap<?, ?>> map3 = new HashMap<String, HashMap<?, ?>>();
    map2.put("map3", (HashMap<?, ?>) map3);

    final ORecordId rid = (ORecordId) newDoc.save().getIdentity();

    final ODocument loadedDoc = database.load(rid);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map1"));
    Assert.assertTrue(loadedDoc.field("map1") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap1 = loadedDoc.field("map1");
    Assert.assertEquals(loadedMap1.size(), 1);

    Assert.assertTrue(loadedMap1.containsKey("map2"));
    Assert.assertTrue(loadedMap1.get("map2") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap2 = (Map<String, ODocument>) loadedMap1.get("map2");
    Assert.assertEquals(loadedMap2.size(), 1);

    Assert.assertTrue(loadedMap2.containsKey("map3"));
    Assert.assertTrue(loadedMap2.get("map3") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap3 = (Map<String, ODocument>) loadedMap2.get("map3");
    Assert.assertEquals(loadedMap3.size(), 0);
  }

  @Test
  public void commandWithPositionalParameters() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
    List<ODocument> result = database.command(query).execute("Barack", "Obama");

    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithPositionalParameters() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
      List<ODocument> result = database.query(query, "Barack", "Obama");

      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test
  public void commandWithNamedParameters() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
          "select from Profile where name = :name and surname = :surname");

      HashMap<String, String> params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<ODocument> result = database.command(query).execute(params);

      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test
  public void commandWrongParameterNames() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      ODocument doc = database.newInstance();

      try {
        doc.field("a:b", 10);
        Assert.assertFalse(true);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }

      try {
        doc.field("a,b", 10);
        Assert.assertFalse(true);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }

    } finally {
      database.close();
    }
  }

  @Test
  public void queryWithNamedParameters() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
          "select from Profile where name = :name and surname = :surname");

      HashMap<String, String> params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<ODocument> result = database.query(query, params);

      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  // @Test
  // public void testTransientField() {
  // database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
  //
  // ODocument doc = new ODocument( "Profile");
  // doc.field("nick", "LucaPhotoTest");
  // doc.field("photo", "testPhoto"); // THIS IS DECLARED TRANSIENT IN SCHEMA
  // (see SchemaTest.java)
  // doc.save();
  //
  // // RELOAD FROM THE CACHE
  // doc.reload(null, false);
  // Assert.assertEquals(doc.field("nick"), "LucaPhotoTest");
  // Assert.assertTrue(doc.containsField("photo"));
  //
  // // RELOAD FROM DISK
  // doc.reload();
  // Assert.assertEquals(doc.field("nick"), "LucaPhotoTest");
  // Assert.assertFalse(doc.containsField("photo")); // THIS IS DECLARED
  // TRANSIENT IN SCHEMA (see SchemaTest.java)
  //
  // database.close();
  // }
  //
  @Test
  public void testDirtyChild() {
    ODocument parent = new ODocument();

    ODocument child1 = new ODocument().addOwner(parent);
    parent.field("child1", child1);

    Assert.assertTrue(child1.hasOwners());

    ODocument child2 = new ODocument().addOwner(child1);
    child1.field("child2", child2);

    Assert.assertTrue(child2.hasOwners());

    // BEFORE FIRST TOSTREAM
    Assert.assertTrue(parent.isDirty());
    parent.toStream();
    // AFTER TOSTREAM
    Assert.assertTrue(parent.isDirty());
    // CHANGE FIELDS VALUE (Automaticaly set dirty this child)
    child1.field("child2", new ODocument());
    Assert.assertTrue(parent.isDirty());
  }

  @Test
  public void testInvalidFetchplanLoad() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    ODocument doc = database.newInstance();
    doc.field("test", "test");
    doc.save();
    ORID docRid = doc.getIdentity().copy();

    try {
      // RELOAD THE DOCUMENT, THIS WILL PUT IT IN L1 CACHE
      doc = database.load(docRid, "*:-1");
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, docRid);
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(4, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(4, OClusterPositionFactory.INSTANCE.valueOf(1)));
      // CLOSE DB AND RE-TEST THE LOAD TO MAKE SURE
    } finally {
      database.close();
    }
    database = null;
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
    try {
      doc = testInvalidFetchPlanClearL1Cache(doc, docRid);
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, OClusterPositionFactory.INSTANCE.valueOf(2)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(4, OClusterPositionFactory.INSTANCE.valueOf(0)));
      doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(4, OClusterPositionFactory.INSTANCE.valueOf(1)));
      doc = database.load(docRid);
      doc.delete();
    } finally {
      database.close();
    }
  }

  private ODocument testInvalidFetchPlanInvalidateL1Cache(ODocument doc, ORID docRid) {
    try {
      // LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L1 CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    // INVALIDATE L1 CACHE TO CHECK THE L2 CACHE
    database.getLevel1Cache().invalidate();
    try {
      // LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L2 CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    // CLEAR THE L2 CACHE TO CHECK THE RAW READ
    database.getLevel2Cache().clear();
    try {
      // LOAD DOCUMENT NOT IN ANY CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    return doc;
  }

  private ODocument testInvalidFetchPlanClearL1Cache(ODocument doc, ORID docRid) {
    try {
      // LOAD DOCUMENT NOT IN ANY CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    // LOAD DOCUMENT, THIS WILL PUT IT IN L1 CACHE
    try {
      database.load(docRid);
    } catch (Exception e) {
    }
    try {
      // LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L1 CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    // CLEAR L1 CACHE, THIS WILL PUT IT IN L2 CACHE
    database.getLevel1Cache().clear();
    try {
      // LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L2 CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    // CLEAR THE L2 CACHE TO CHECK THE RAW READ
    database.getLevel2Cache().clear();
    try {
      // LOAD DOCUMENT NOT IN ANY CACHE
      doc = database.load(docRid, "invalid");
      Assert.fail("Should throw IllegalArgumentException");
    } catch (Exception e) {
    }
    return doc;
  }

  public void testEncoding() {
    String s = " \r\n\t:;,.|+*/\\=!?[]()'\"";

    ODocument doc = new ODocument();
    doc.field("test", s);
    doc.unpin();
    doc.save();

    doc.reload(null, true);
    Assert.assertEquals(doc.field("test"), s);
  }

  @Test
  public void polymorphicQuery() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    final ORecordAbstract<Object> newAccount = new ODocument("Account").field("name", "testInheritanceName").save();

    List<ODocument> superClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Account"));
    List<ODocument> subClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Company"));

    Assert.assertTrue(superClassResult.size() != 0);
    Assert.assertTrue(subClassResult.size() != 0);
    Assert.assertTrue(superClassResult.size() >= subClassResult.size());

    // VERIFY ALL THE SUBCLASS RESULT ARE ALSO CONTAINED IN SUPERCLASS
    // RESULT
    for (ODocument d : subClassResult) {
      Assert.assertTrue(superClassResult.contains(d));
    }

    HashSet<ODocument> browsed = new HashSet<ODocument>();
    for (ODocument d : database.browseClass("Account")) {
      Assert.assertFalse(browsed.contains(d));
      browsed.add(d);
    }

    newAccount.delete();

    database.close();
  }

  @Test(dependsOnMethods = "testCreate")
  public void testBrowseClassHasNextTwice() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    ODocument doc1 = null;
    for (Iterator<ODocument> itDoc = database.browseClass("Account"); itDoc.hasNext();) {
      doc1 = itDoc.next();
      break;
    }

    ODocument doc2 = null;
    for (Iterator<ODocument> itDoc = database.browseClass("Account"); itDoc.hasNext();) {
      itDoc.hasNext();
      doc2 = itDoc.next();
      break;
    }

    Assert.assertEquals(doc1, doc2);

    database.close();
  }

  @Test(dependsOnMethods = "testCreate")
  public void nonPolymorphicQuery() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    final ORecordAbstract<Object> newAccount = new ODocument("Account").field("name", "testInheritanceName").save();

    List<ODocument> allResult = database.query(new OSQLSynchQuery<ODocument>("select from Account"));
    List<ODocument> superClassResult = database
        .query(new OSQLSynchQuery<ODocument>("select from Account where @class = 'Account'"));
    List<ODocument> subClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Company where @class = 'Company'"));

    Assert.assertTrue(allResult.size() != 0);
    Assert.assertTrue(superClassResult.size() != 0);
    Assert.assertTrue(subClassResult.size() != 0);

    // VERIFY ALL THE SUBCLASS RESULT ARE NOT CONTAINED IN SUPERCLASS RESULT
    for (ODocument d : subClassResult) {
      Assert.assertFalse(superClassResult.contains(d));
    }

    HashSet<ODocument> browsed = new HashSet<ODocument>();
    for (ODocument d : database.browseClass("Account")) {
      Assert.assertFalse(browsed.contains(d));
      browsed.add(d);
    }

    newAccount.delete();

    database.close();
  }

  @Test(dependsOnMethods = "cleanAll")
  public void asynchInsertion() {
    ODatabaseDocumentPool pool = new ODatabaseDocumentPool(url, "admin", "admin");

    database = pool.acquire();

    try {
      startRecordNumber = database.countClusterElements("Account");
      final AtomicInteger callBackCalled = new AtomicInteger();

      final long total = startRecordNumber + TOT_RECORDS;
      for (long i = startRecordNumber; i < total; ++i) {
        record.reset();
        record.setClassName("Account");

        record.field("id", i);
        record.field("name", "Asynch insertion test");
        record.field("location", "Italy");
        record.field("salary", (i + 300));

        database.save(record, OPERATION_MODE.ASYNCHRONOUS, false, new ORecordCallback<OClusterPosition>() {

          public void call(ORecordId iRID, OClusterPosition iParameter) {
            callBackCalled.incrementAndGet();
          }
        }, null);
      }

      while (callBackCalled.intValue() < total) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }

      Assert.assertEquals(callBackCalled.intValue(), total);

      // WAIT UNTIL ALL RECORD ARE INSERTED. USE A NEW DATABASE CONNECTION
      // TO AVOID TO ENQUEUE THE COUNT ITSELF
      final ODatabaseDocumentTx db = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
      long tot;
      while ((tot = db.countClusterElements("Account")) < startRecordNumber + TOT_RECORDS) {
        System.out.println("Asynchronous insertion: found " + tot + " records but waiting till "
            + (startRecordNumber + TOT_RECORDS) + " is reached");
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
      db.close();

      while (database.countClusterElements("Account") > 0)
        for (ODocument d : database.browseClass("Account")) {
          if (d.field("name").equals("Asynch insertion test"))
            d.delete();
        }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "cleanAll")
  public void testEmbeddeDocumentInTx() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    ODocument bank = database.newInstance("Account");

    try {
      database.begin();

      bank.field("Name", "MyBank");

      ODocument bank2 = database.newInstance("Account");
      bank.field("embedded", bank2, OType.EMBEDDED);
      bank.save();

      database.commit();

    } finally {
      database.close();
    }

    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
    try {
      bank.reload();
      Assert.assertTrue(((ODocument) bank.field("embedded")).isEmbedded());
      Assert.assertFalse(((ODocument) bank.field("embedded")).getIdentity().isPersistent());

      bank.delete();

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "cleanAll")
  public void testUpdateInChain() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    ODocument bank = database.newInstance("Account");
    try {
      bank.field("name", "MyBankChained");

      // EMBEDDED
      ODocument embedded = database.newInstance("Account").field("name", "embedded1");
      bank.field("embedded", embedded, OType.EMBEDDED);

      ODocument[] embeddeds = new ODocument[] { database.newInstance("Account").field("name", "embedded2"),
          database.newInstance("Account").field("name", "embedded3") };
      bank.field("embeddeds", embeddeds, OType.EMBEDDEDLIST);

      // LINKED
      ODocument linked = database.newInstance("Account").field("name", "linked1");
      bank.field("linked", linked);

      ODocument[] linkeds = new ODocument[] { database.newInstance("Account").field("name", "linked2"),
          database.newInstance("Account").field("name", "linked3") };
      bank.field("linkeds", linkeds, OType.LINKLIST);

      bank.save();

    } finally {
      database.close();
    }

    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
    try {
      bank.reload();

      ODocument changedDoc1 = bank.field("embedded.total", 100);
      // MUST CHANGE THE PARENT DOC BECAUSE IT'S EMBEDDED
      Assert.assertEquals(changedDoc1.field("name"), "MyBankChained");
      Assert.assertEquals(changedDoc1.field("embedded.total"), 100);

      ODocument changedDoc2 = bank.field("embeddeds.total", 200);
      // MUST CHANGE THE PARENT DOC BECAUSE IT'S EMBEDDED
      Assert.assertEquals(changedDoc2.field("name"), "MyBankChained");
      Collection<Integer> embeddeds = changedDoc2.field("embeddeds.total");
      for (Integer e : embeddeds)
        Assert.assertEquals(e.intValue(), 200);

      ODocument changedDoc3 = bank.field("linked.total", 300);
      // MUST CHANGE THE LINKED DOCUMENT
      Assert.assertEquals(changedDoc3.field("name"), "linked1");
      Assert.assertEquals(changedDoc3.field("total"), 300);

      try {
        bank.field("linkeds.total", 400);
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        // MUST THROW AN EXCEPTION
        Assert.assertTrue(true);
      }

      ((ODocument) bank.field("linked")).delete();
      for (ODocument l : (Collection<ODocument>) bank.field("linkeds"))
        l.delete();
      bank.delete();

    } finally {
      database.close();
    }
  }

  public void testSerialization() {
    final byte[] streamOrigin = "Account@html:{\"path\":\"html/layout\"},config:{\"title\":\"Github Admin\",\"modules\":(githubDisplay:\"github_display\")},complex:(simple1:\"string1\",one_level1:(simple2:\"string2\"),two_levels:(simple3:\"string3\",one_level2:(simple4:\"string4\")))"
        .getBytes();
    ODocument doc = new ODocument().fromStream(streamOrigin);
    doc.field("out");
    final byte[] streamDest = doc.toStream();
    Assert.assertEquals(streamOrigin, streamDest);
  }

  public void testUpdateNoVersionCheck() {
    database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from Account"));
      ODocument doc = result.get(0);
      doc.field("name", "modified");
      int oldVersion = doc.getVersion();

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.setCounter(-2);
      doc.getRecordVersion().copyFrom(recordVersion);

      doc.save();

      doc.reload();
      Assert.assertEquals(doc.getVersion(), oldVersion);
      Assert.assertEquals(doc.field("name"), "modified");
    } finally {
      database.close();
    }
  }
}
