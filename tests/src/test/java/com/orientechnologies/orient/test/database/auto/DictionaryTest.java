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

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

@Test(groups = "dictionary")
public class DictionaryTest extends DocumentDBBaseTest {
  public DictionaryTest() {
  }

  @Parameters(value = "url")
  public DictionaryTest(@Optional String url) {
    super(url);
  }

  public void testDictionaryCreate() throws IOException {
    ODocument record = new ODocument();

    database.getDictionary().put("key1", record.field("test", "Dictionary test!"));
  }

  @Test(dependsOnMethods = "testDictionaryCreate")
  public void testDictionaryLookup() throws IOException {
    Assert.assertNotNull(database.getDictionary().get("key1"));
    Assert.assertTrue(((ODocument) database.getDictionary().get("key1")).field("test").equals("Dictionary test!"));
  }

  @Test(dependsOnMethods = "testDictionaryLookup")
  public void testDictionaryUpdate() throws IOException {
    final long originalSize = database.getDictionary().size();

    database.getDictionary().put("key1", new ODocument().field("test", "Text changed"));

    database.close();
    database.open("admin", "admin");

    Assert.assertEquals(((ODocument) database.getDictionary().get("key1")).field("test"), "Text changed");
    Assert.assertEquals(database.getDictionary().size(), originalSize);
  }

  @Test(dependsOnMethods = "testDictionaryUpdate")
  public void testDictionaryDelete() throws IOException {
    final long originalSize = database.getDictionary().size();
    Assert.assertNotNull(database.getDictionary().remove("key1"));

    database.close();
    database.open("admin", "admin");

    Assert.assertEquals(database.getDictionary().size(), originalSize - 1);
  }

  @Test(dependsOnMethods = "testDictionaryDelete")
  public void testDictionaryMassiveCreate() throws IOException {
    final long originalSize = database.getDictionary().size();

    // ASSURE TO STORE THE PAGE-SIZE + 3 FORCING THE CREATION OF LEFT AND RIGHT
    final int total = 1000;

    for (int i = total; i > 0; --i) {
      database.getDictionary().put("key-" + (originalSize + i), new ODocument().field("test", "test-dictionary-" + i));
    }

    for (int i = total; i > 0; --i) {
      ORecord record = database.getDictionary().get("key-" + (originalSize + i));
      record.toString().equals("test-dictionary-" + i);
    }

    Assert.assertEquals(database.getDictionary().size(), originalSize + total);
  }

  @Test(dependsOnMethods = "testDictionaryMassiveCreate")
  public void testDictionaryInTx() throws IOException {
    database.begin();
    database.getDictionary().put("tx-key", new ODocument().field("test", "tx-test-dictionary"));
    database.commit();

    Assert.assertNotNull(database.getDictionary().get("tx-key"));
  }

  public class ObjectDictionaryTest {
    private String name;

    public ObjectDictionaryTest() {
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @Test(dependsOnMethods = "testDictionaryMassiveCreate")
  public void testDictionaryWithPOJOs() throws IOException {
    OObjectDatabaseTx database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");
    database.getEntityManager().registerEntityClass(ObjectDictionaryTest.class);

    Assert.assertNull(database.getDictionary().get("testKey"));
    database.getDictionary().put("testKey", new ObjectDictionaryTest());
    Assert.assertNotNull(database.getDictionary().get("testKey"));

    database.close();
  }

  @Test(dependsOnMethods = "testDictionaryMassiveCreate")
  public void testIndexManagerReloadReloadsDictionary() throws IOException {
    ODatabaseDocumentTx database1 = new ODatabaseDocumentTx(url);
    database1.open("admin", "admin");

    Assert.assertNull(database1.getDictionary().get("testReloadKey"));

    ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(url);
    database2.open("admin", "admin");
    database2.getMetadata().getIndexManager().reload();
    database2.getDictionary().put("testReloadKey", new ODocument().field("testField", "a"));

    database1.activateOnCurrentThread();
    Assert.assertEquals(database1.getDictionary().<ODocument> get("testReloadKey").field("testField"), "a");
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
  }
}
