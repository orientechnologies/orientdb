/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
@Test(groups = {"crud", "record-vobject"})
public class ComplexTypesTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public ComplexTypesTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testBigDecimal() {
    ODocument newDoc = new ODocument();
    newDoc.field("integer", new BigInteger("10"));
    newDoc.field("decimal_integer", new BigDecimal(10));
    newDoc.field("decimal_float", new BigDecimal("10.34"));
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertEquals(((Number) loadedDoc.field("integer")).intValue(), 10);
    Assert.assertEquals(loadedDoc.field("decimal_integer"), new BigDecimal(10));
    Assert.assertEquals(loadedDoc.field("decimal_float"), new BigDecimal("10.34"));
  }

  @Test
  public void testEmbeddedList() {
    ODocument newDoc = new ODocument();

    final ArrayList<ODocument> list = new ArrayList<ODocument>();
    newDoc.field("embeddedList", list, OType.EMBEDDEDLIST);
    list.add(new ODocument().field("name", "Luca"));
    list.add(new ODocument("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<ODocument>) loadedDoc.field("embeddedList")).get(0) instanceof ODocument);

    ODocument d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(d.getClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testLinkList() {
    ODocument newDoc = new ODocument();

    final ArrayList<ODocument> list = new ArrayList<ODocument>();
    newDoc.field("linkedList", list, OType.LINKLIST);
    list.add(
        new ODocument()
            .field("name", "Luca")
            .save(database.getClusterNameById(database.getDefaultClusterId())));
    list.add(new ODocument("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedList"));
    Assert.assertTrue(loadedDoc.field("linkedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<ODocument>) loadedDoc.field("linkedList")).get(0) instanceof ODocument);

    ODocument d = ((List<ODocument>) loadedDoc.field("linkedList")).get(0);
    Assert.assertTrue(d.getIdentity().isValid());
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<ODocument>) loadedDoc.field("linkedList")).get(1);
    Assert.assertEquals(d.getClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testEmbeddedSet() {
    ODocument newDoc = new ODocument();

    final Set<ODocument> set = new HashSet<ODocument>();
    newDoc.field("embeddedSet", set, OType.EMBEDDEDSET);
    set.add(new ODocument().field("name", "Luca"));
    set.add(new ODocument("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedSet"));
    Assert.assertTrue(loadedDoc.field("embeddedSet", Set.class) instanceof Set<?>);

    final Iterator<ODocument> it =
        ((Collection<ODocument>) loadedDoc.field("embeddedSet")).iterator();

    int tot = 0;
    while (it.hasNext()) {
      ODocument d = it.next();
      Assert.assertTrue(d instanceof ODocument);

      if (d.field("name").equals("Marcus")) Assert.assertEquals(d.getClassName(), "Account");

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testLinkSet() {
    ODocument newDoc = new ODocument();

    final Set<ODocument> set = new HashSet<ODocument>();
    newDoc.field("linkedSet", set, OType.LINKSET);
    set.add(
        new ODocument()
            .field("name", "Luca")
            .save(database.getClusterNameById(database.getDefaultClusterId())));
    set.add(new ODocument("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedSet"));
    Assert.assertTrue(loadedDoc.field("linkedSet", Set.class) instanceof Set<?>);

    final Iterator<ODocument> it =
        ((Collection<ODocument>) loadedDoc.field("linkedSet")).iterator();

    int tot = 0;
    while (it.hasNext()) {
      ODocument d = it.next();
      Assert.assertTrue(d instanceof ODocument);

      if (d.field("name").equals("Marcus")) Assert.assertEquals(d.getClassName(), "Account");

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    newDoc.field("embeddedMap", map, OType.EMBEDDEDMAP);
    map.put("Luca", new ODocument().field("name", "Luca"));
    map.put("Marcus", new ODocument().field("name", "Marcus"));
    map.put("Cesare", new ODocument("Account").field("name", "Cesare"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, ODocument>) loadedDoc.field("embeddedMap")).values().iterator().next()
            instanceof ODocument);

    ODocument d = ((Map<String, ODocument>) loadedDoc.field("embeddedMap")).get("Luca");
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, ODocument>) loadedDoc.field("embeddedMap")).get("Marcus");
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, ODocument>) loadedDoc.field("embeddedMap")).get("Cesare");
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getClassName(), "Account");
  }

  @Test
  public void testEmptyEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    newDoc.field("embeddedMap", map, OType.EMBEDDEDMAP);

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);

    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, ODocument> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testLinkMap() {
    ODocument newDoc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    newDoc.field("linkedMap", map, OType.LINKMAP);
    map.put(
        "Luca",
        new ODocument()
            .field("name", "Luca")
            .save(database.getClusterNameById(database.getDefaultClusterId())));
    map.put(
        "Marcus",
        new ODocument()
            .field("name", "Marcus")
            .save(database.getClusterNameById(database.getDefaultClusterId())));
    map.put("Cesare", new ODocument("Account").field("name", "Cesare"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    final ORID rid = newDoc.getIdentity();

    database.close();
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument loadedDoc = database.load(rid);
    Assert.assertNotNull(loadedDoc.field("linkedMap", OType.LINKMAP));
    Assert.assertTrue(loadedDoc.field("linkedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, ODocument>) loadedDoc.field("linkedMap")).values().iterator().next()
            instanceof ODocument);

    ODocument d = ((Map<String, ODocument>) loadedDoc.field("linkedMap")).get("Luca");
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, ODocument>) loadedDoc.field("linkedMap")).get("Marcus");
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, ODocument>) loadedDoc.field("linkedMap")).get("Cesare");
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getClassName(), "Account");
  }
}
