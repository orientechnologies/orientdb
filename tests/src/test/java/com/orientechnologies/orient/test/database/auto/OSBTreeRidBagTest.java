/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
@Test
public class OSBTreeRidBagTest extends ORidBagTest {
  private int topThreshold;
  private int bottomThreshold;

  @Parameters(value = "url")
  public OSBTreeRidBagTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();
    super.beforeClass();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", ODatabaseHelper.getServerRootPassword());
      server.setGlobalConfiguration(
          OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
      server.setGlobalConfiguration(
          OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", ODatabaseHelper.getServerRootPassword());
      server.setGlobalConfiguration(
          OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, topThreshold);
      server.setGlobalConfiguration(
          OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, bottomThreshold);
      server.close();
    }
  }

  public void testRidBagClusterDistribution() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) return;

    final int clusterIdOne = database.addCluster("clusterOne");

    ODocument docClusterOne = new ODocument();
    ORidBag ridBagClusterOne = new ORidBag();
    docClusterOne.field("ridBag", ridBagClusterOne);
    docClusterOne.save("clusterOne");

    final String directory = database.getStorage().getConfiguration().getDirectory();

    final OWOWCache wowCache =
        (OWOWCache) ((OLocalPaginatedStorage) (database.getStorage())).getWriteCache();

    final long fileId =
        wowCache.fileIdByName(
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterIdOne
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);
    final String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;
    final File ridBagOneFile = new File(directory, fileName);
    Assert.assertTrue(ridBagOneFile.exists());
  }

  public void testIteratorOverAfterRemove() {
    ODocument scuti =
        new ODocument()
            .field("name", "UY Scuti")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument cygni =
        new ODocument()
            .field("name", "NML Cygni")
            .save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument scorpii =
        new ODocument()
            .field("name", "AH Scorpii")
            .save(database.getClusterNameById(database.getDefaultClusterId()));

    HashSet<ODocument> expectedResult = new HashSet<ODocument>();
    expectedResult.addAll(Arrays.asList(scuti, scorpii));

    ORidBag bag = new ORidBag();
    bag.add(scuti);
    bag.add(cygni);
    bag.add(scorpii);

    ODocument doc = new ODocument();
    doc.field("ridBag", bag);
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    bag.remove(cygni);

    Set<ODocument> result = new HashSet<ODocument>();
    for (OIdentifiable identifiable : bag) {
      result.add((ODocument) identifiable.getRecord());
    }

    Assert.assertEquals(result, expectedResult);
  }

  public void testRidBagConversion() {
    final int oldThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);

    ODocument doc_1 = new ODocument();
    doc_1.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument doc_2 = new ODocument();
    doc_2.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument doc_3 = new ODocument();
    doc_3.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument doc_4 = new ODocument();
    doc_4.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument doc = new ODocument();

    ORidBag bag = new ORidBag();
    bag.add(doc_1);
    bag.add(doc_2);
    bag.add(doc_3);
    bag.add(doc_4);

    doc.field("ridBag", bag);
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    doc.reload();

    ODocument doc_5 = new ODocument();
    doc_5.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument doc_6 = new ODocument();
    doc_6.save(database.getClusterNameById(database.getDefaultClusterId()));

    bag = doc.field("ridBag");
    bag.add(doc_5);
    bag.add(doc_6);

    doc.save();
    doc.reload();

    bag = doc.field("ridBag");
    Assert.assertEquals(bag.size(), 6);

    List<OIdentifiable> docs = new ArrayList<OIdentifiable>();

    docs.add(doc_1.getIdentity());
    docs.add(doc_2.getIdentity());
    docs.add(doc_3.getIdentity());
    docs.add(doc_4.getIdentity());
    docs.add(doc_5.getIdentity());
    docs.add(doc_6.getIdentity());

    for (OIdentifiable rid : bag) Assert.assertTrue(docs.remove(rid));

    Assert.assertTrue(docs.isEmpty());

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(oldThreshold);
  }

  public void testRidBagDelete() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) return;

    float reuseTrigger =
        OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.getValueAsFloat();
    OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(Float.MIN_VALUE);

    ODocument realDoc = new ODocument();
    ORidBag realDocRidBag = new ORidBag();
    realDoc.field("ridBag", realDocRidBag);

    for (int i = 0; i < 10; i++) {
      ODocument docToAdd = new ODocument();
      realDocRidBag.add(docToAdd);
    }

    assertEmbedded(realDocRidBag.isEmbedded());
    realDoc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final int clusterId = database.addCluster("ridBagDeleteTest");

    ODocument testDocument = crateTestDeleteDoc(realDoc);
    database.freeze();
    database.release();

    final String directory = database.getStorage().getConfiguration().getDirectory();

    File testRidBagFile =
        new File(
            directory,
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterId
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);
    long testRidBagSize = testRidBagFile.length();

    for (int i = 0; i < 100; i++) {
      testDocument.reload();

      testDocument.delete();
      testDocument = crateTestDeleteDoc(realDoc);
    }

    database.freeze();
    database.release();

    OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(reuseTrigger);
    testRidBagFile =
        new File(
            directory,
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterId
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);

    Assert.assertEquals(testRidBagFile.length(), testRidBagSize);

    realDoc = database.load(realDoc.getIdentity(), "*:*", true);
    ORidBag ridBag = realDoc.field("ridBag");
    Assert.assertEquals(ridBag.size(), 10);
  }

  private ODocument crateTestDeleteDoc(ODocument realDoc) {
    ODocument testDocument = new ODocument();
    ORidBag highLevelRidBag = new ORidBag();
    testDocument.field("ridBag", highLevelRidBag);
    testDocument.field("realDoc", realDoc);

    testDocument.save("ridBagDeleteTest");

    return testDocument;
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue((!isEmbedded || ODatabaseRecordThreadLocal.instance().get().isRemote()));
  }
}
