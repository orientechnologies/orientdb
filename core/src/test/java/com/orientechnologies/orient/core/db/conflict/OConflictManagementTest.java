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
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

public class OConflictManagementTest extends DatabaseAbstractTest {

  @Test
  public void testDefaultStrategy() {
    final ODocument rootDoc =
        new ODocument()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    final ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    copy.field("name", "Jay2");
    try {
      copy.save(database.getClusterNameById(database.getDefaultClusterId()));
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  @Test
  public void testVersionStrategy() {
    database.setConflictStrategy("version");
    ODocument rootDoc =
        new ODocument()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    copy.field("name", "Jay2");
    try {
      copy.save(database.getClusterNameById(database.getDefaultClusterId()));
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  @Test
  public void testContentStrategy() {
    database.setConflictStrategy("content");
    ODocument rootDoc =
        new ODocument()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    copy.field("name", "Jay1");
    copy.save(database.getClusterNameById(database.getDefaultClusterId()));
  }

  @Test
  public void testAutomergeStrategy() {
    database.setConflictStrategy("automerge");
    ODocument rootDoc =
        new ODocument()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    copy.field("name", "Jay1");
    copy.save(database.getClusterNameById(database.getDefaultClusterId()));
  }

  @Test
  public void testAutomergeStrategyWithLinks() {
    database.setConflictStrategy("automerge");
    ODocument rootDoc =
        new ODocument()
            .field("name", "Jay")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument linkedDoc =
        new ODocument()
            .field("product", "Amiga")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    rootDoc.field("relationships", new OIdentifiable[] {linkedDoc}, OType.LINKSET);
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();
    ORecordInternal.setDirtyManager(copy, new ODirtyManager());
    ORecordInternal.getDirtyManager(rootDoc).clear();

    ODocument linkedDoc2 =
        new ODocument()
            .field("company", "Commodore")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    rootDoc.field("relationships", new OIdentifiable[] {linkedDoc, linkedDoc2}, OType.LINKSET);
    rootDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument linkedDoc3 =
        new ODocument()
            .field("company", "Atari")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    copy.field("relationships", new OIdentifiable[] {linkedDoc, linkedDoc3}, OType.LINKSET);
    copy.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument reloadedDoc = (ODocument) rootDoc.reload();
    Assert.assertEquals(((Collection) reloadedDoc.field("relationships")).size(), 3);
    Collection<OIdentifiable> rels = reloadedDoc.field("relationships");
    Assert.assertTrue(rels.contains(linkedDoc));
    Assert.assertTrue(rels.contains(linkedDoc2));
    Assert.assertTrue(rels.contains(linkedDoc3));
  }
}
