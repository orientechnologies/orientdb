/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.core.db.conflict;

import java.util.Collection;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test
public class OConflictManagementTest extends DatabaseAbstractTest {
  @BeforeClass
  public void setUp() throws Exception {
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
  }

  public void testDefaultStrategy() {
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay2");
    try {
      copy.save();
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  public void testVersionStrategy() {
    database.setConflictStrategy("version");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay2");
    try {
      copy.save();
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  public void testContentStrategy() {
    database.setConflictStrategy("content");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay1");
    copy.save();
  }

  public void testAutomergeStrategy() {
    database.setConflictStrategy("automerge");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay1");
    copy.save();
  }

  public void testAutomergeStrategyWithLinks() {
    database.setConflictStrategy("automerge");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();
    ODocument linkedDoc = new ODocument().field("product", "Amiga").save();
    rootDoc.field("relationships", new OIdentifiable[] { linkedDoc }, OType.LINKSET);
    rootDoc.save();

    ODocument copy = rootDoc.copy();

    ODocument linkedDoc2 = new ODocument().field("company", "Commodore").save();
    rootDoc.field("relationships", new OIdentifiable[] { linkedDoc, linkedDoc2 }, OType.LINKSET);
    rootDoc.save();

    ODocument linkedDoc3 = new ODocument().field("company", "Atari").save();
    copy.field("relationships", new OIdentifiable[] { linkedDoc, linkedDoc3 }, OType.LINKSET);
    copy.save();

    ODocument reloadedDoc = (ODocument) rootDoc.reload();
    Assert.assertEquals(((Collection) reloadedDoc.field("relationships")).size(), 3);
    Collection<OIdentifiable> rels = reloadedDoc.field("relationships");
    Assert.assertTrue(rels.contains(linkedDoc));
    Assert.assertTrue(rels.contains(linkedDoc2));
    Assert.assertTrue(rels.contains(linkedDoc3));
  }
}
