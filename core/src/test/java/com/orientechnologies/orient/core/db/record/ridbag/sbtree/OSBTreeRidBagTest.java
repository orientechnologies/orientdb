/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.io.File;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagTest;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test
public class OSBTreeRidBagTest extends ORidBagTest {
  private int topThreshold;
  private int bottomThreshold;

  @BeforeMethod
  public void beforeMethod() {
    topThreshold = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @AfterMethod
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  public void testRidBagClusterDistribution() {
    final int clusterIdOne = db.addCluster("clusterOne", OStorage.CLUSTER_TYPE.PHYSICAL);
    final int clusterIdTwo = db.addCluster("clusterTwo", OStorage.CLUSTER_TYPE.PHYSICAL);
    final int clusterIdThree = db.addCluster("clusterThree", OStorage.CLUSTER_TYPE.PHYSICAL);
    final int clusterIdFour = db.addCluster("clusterFour", OStorage.CLUSTER_TYPE.PHYSICAL);

    ODocument docClusterOne = new ODocument();
    ORidBag ridBagClusterOne = new ORidBag();
    docClusterOne.field("ridBag", ridBagClusterOne);
    docClusterOne.save("clusterOne");

    ODocument docClusterTwo = new ODocument();

    ODocument embeddedDocTwo = new ODocument();
    final ORidBag ridBagClusterTwo = new ORidBag();

    embeddedDocTwo.field("ridBag", ridBagClusterTwo);
    List<ODocument> elist = new ArrayList<ODocument>();
    elist.add(embeddedDocTwo);

    docClusterTwo.field("elist", elist, OType.EMBEDDEDLIST);
    docClusterTwo.save("clusterTwo");

    ODocument docClusterThree = new ODocument();

    ODocument embeddedDocThree = new ODocument();
    final ORidBag ridBagClusterThree = new ORidBag();

    embeddedDocThree.field("ridBag", ridBagClusterThree);
    Set<ODocument> eset = new HashSet<ODocument>();
    eset.add(embeddedDocThree);

    docClusterThree.field("eset", eset, OType.EMBEDDEDSET);
    docClusterThree.save("clusterThree");

    ODocument docClusterFour = new ODocument();

    ODocument embeddedDocFour = new ODocument();
    final ORidBag ridBagClusterFour = new ORidBag();

    embeddedDocFour.field("ridBag", ridBagClusterFour);
    Map<String, ODocument> emap = new HashMap<String, ODocument>();
    emap.put("l", embeddedDocFour);

    docClusterFour.field("emap", emap, OType.EMBEDDEDMAP);
    docClusterFour.save("clusterFour");

    final String directory = db.getStorage().getConfiguration().getDirectory();
    final File ridBagOneFile = new File(directory, OSBTreeCollectionManagerShared.FILE_NAME_PREFIX + clusterIdOne
        + OSBTreeCollectionManagerShared.DEFAULT_EXTENSION);
    Assert.assertTrue(ridBagOneFile.exists());

    final File ridBagTwoFile = new File(directory, OSBTreeCollectionManagerShared.FILE_NAME_PREFIX + clusterIdTwo
        + OSBTreeCollectionManagerShared.DEFAULT_EXTENSION);
    Assert.assertTrue(ridBagTwoFile.exists());

    final File ridBagThreeFile = new File(directory, OSBTreeCollectionManagerShared.FILE_NAME_PREFIX + clusterIdThree
        + OSBTreeCollectionManagerShared.DEFAULT_EXTENSION);
    Assert.assertTrue(ridBagThreeFile.exists());

    final File ridBagFourFile = new File(directory, OSBTreeCollectionManagerShared.FILE_NAME_PREFIX + clusterIdFour
        + OSBTreeCollectionManagerShared.DEFAULT_EXTENSION);
    Assert.assertTrue(ridBagFourFile.exists());
  }

  @Test
  public void testSizeNotChangeAfterRemoveNotExistentElement() throws Exception {
    final ODocument bob = new ODocument();
    final ODocument fred = new ODocument().save();
    final ODocument jim = new ODocument().save();

    ORidBag teamMates = new ORidBag();

    teamMates.add(bob);
    teamMates.add(fred);

    Assert.assertEquals(teamMates.size(), 2);

    teamMates.remove(jim);

    Assert.assertEquals(teamMates.size(), 2);
  }

  @Test
  public void testRemoveNotExistentElementAndAddIt() throws Exception {
    ORidBag teamMates = new ORidBag();

    final ODocument bob = new ODocument().save();

    teamMates.remove(bob);

    Assert.assertEquals(teamMates.size(), 0);

    teamMates.add(bob);

    Assert.assertEquals(teamMates.size(), 1);
    Assert.assertEquals(teamMates.iterator().next().getIdentity(), bob.getIdentity());
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue(!isEmbedded);
  }
}
