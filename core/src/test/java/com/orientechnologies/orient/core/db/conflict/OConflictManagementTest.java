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

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

public class OConflictManagementTest extends BaseMemoryDatabase {

  @Test
  public void testDefaultStrategy() {
    final ODocument rootDoc =
        new ODocument().field("name", "Jay").save(db.getClusterNameById(db.getDefaultClusterId()));
    final ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    copy.field("name", "Jay2");
    try {
      copy.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  @Test
  public void testVersionStrategy() {
    db.setConflictStrategy("version");
    ODocument rootDoc =
        new ODocument().field("name", "Jay").save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    copy.field("name", "Jay2");
    try {
      copy.save(db.getClusterNameById(db.getDefaultClusterId()));
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
    }
  }

  @Test
  public void testContentStrategy() {
    db.setConflictStrategy("content");
    ODocument rootDoc =
        new ODocument().field("name", "Jay").save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    copy.field("name", "Jay1");
    copy.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  @Test
  public void testAutomergeStrategy() {
    db.setConflictStrategy("automerge");
    ODocument rootDoc =
        new ODocument().field("name", "Jay").save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();

    rootDoc.field("name", "Jay1");
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    copy.field("name", "Jay1");
    copy.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  @Test
  public void testAutomergeStrategyWithLinks() {
    db.setConflictStrategy("automerge");
    ODocument rootDoc =
        new ODocument().field("name", "Jay").save(db.getClusterNameById(db.getDefaultClusterId()));
    ODocument linkedDoc =
        new ODocument()
            .field("product", "Amiga")
            .save(db.getClusterNameById(db.getDefaultClusterId()));
    rootDoc.field("relationships", new OIdentifiable[] {linkedDoc}, OType.LINKSET);
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument copy = rootDoc.copy();
    ORecordInternal.setDirtyManager(copy, new ODirtyManager());
    ORecordInternal.getDirtyManager(rootDoc).clear();

    ODocument linkedDoc2 =
        new ODocument()
            .field("company", "Commodore")
            .save(db.getClusterNameById(db.getDefaultClusterId()));
    rootDoc.field("relationships", new OIdentifiable[] {linkedDoc, linkedDoc2}, OType.LINKSET);
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument linkedDoc3 =
        new ODocument()
            .field("company", "Atari")
            .save(db.getClusterNameById(db.getDefaultClusterId()));
    copy.field("relationships", new OIdentifiable[] {linkedDoc, linkedDoc3}, OType.LINKSET);
    copy.save(db.getClusterNameById(db.getDefaultClusterId()));

    ODocument reloadedDoc = (ODocument) rootDoc.reload();
    Assert.assertEquals(((Collection) reloadedDoc.field("relationships")).size(), 3);
    Collection<OIdentifiable> rels = reloadedDoc.field("relationships");
    Assert.assertTrue(rels.contains(linkedDoc));
    Assert.assertTrue(rels.contains(linkedDoc2));
    Assert.assertTrue(rels.contains(linkedDoc3));
  }
}
