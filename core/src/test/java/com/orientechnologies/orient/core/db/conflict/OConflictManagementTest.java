/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *  
 */

package com.orientechnologies.orient.core.db.conflict;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class OConflictManagementTest extends DatabaseAbstractTest {

  @Test
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

  @Test
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

  @Test
  public void testContentStrategy() {
    database.setConflictStrategy("content");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay1");
    copy.save();
  }

  @Test
  public void testAutomergeStrategy() {
    database.setConflictStrategy("automerge");
    ODocument rootDoc = new ODocument().field("name", "Jay").save();

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save();

    copy.field("name", "Jay1");
    copy.save();
  }

  @Test
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
