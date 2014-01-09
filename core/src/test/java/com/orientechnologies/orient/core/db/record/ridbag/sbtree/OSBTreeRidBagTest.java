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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagTest;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public void testIteratorOverAfterRemove() {
    ODocument scuti = new ODocument().field("name", "UY Scuti").save();
    ODocument cygni = new ODocument().field("name", "NML Cygni").save();
    ODocument scorpii = new ODocument().field("name", "AH Scorpii").save();

    HashSet<ODocument> expectedResult = new HashSet<ODocument>();
    expectedResult.addAll(Arrays.asList(scuti, scorpii));

    ORidBag bag = new ORidBag();
    bag.add(scuti);
    bag.add(cygni);
    bag.add(scorpii);

    ODocument doc = new ODocument();
    doc.field("ridBag", bag);
    doc.save();

    bag.remove(cygni);

    Set<ODocument> result = new HashSet<ODocument>();
    for (OIdentifiable identifiable : bag) {
      result.add((ODocument) identifiable.getRecord());
    }

    Assert.assertEquals(result, expectedResult);
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue(!isEmbedded);
  }
}
