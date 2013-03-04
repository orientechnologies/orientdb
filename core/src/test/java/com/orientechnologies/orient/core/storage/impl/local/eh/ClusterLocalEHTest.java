package com.orientechnologies.orient.core.storage.impl.local.eh;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 07.02.13
 */
@Test
public class ClusterLocalEHTest {
  private static final int KEYS_COUNT = 500000;

  private OClusterLocalEH  extendibleHashingCluster;

  @Mock
  public OStorageLocal     storageLocal;

  @BeforeClass
  public void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    extendibleHashingCluster = new OClusterLocalEH(46, 25, new OClusterPositionNodeId(ONodeId.ZERO),
        new OClusterPositionFactory.OClusterPositionFactoryNodeId(), false);

    MockitoAnnotations.initMocks(this);
    when(storageLocal.getMode()).thenReturn("rw");
    when(storageLocal.getVariableParser()).thenReturn(new OStorageVariableParser(buildDirectory));
    when(storageLocal.getConfiguration()).thenReturn(new OStorageConfiguration(storageLocal));
    extendibleHashingCluster.configure(storageLocal, 1, "ehtest", "", 1);
    extendibleHashingCluster.create(-1);
  }

  @AfterClass
  public void afterClass() throws IOException {
    extendibleHashingCluster.delete();
  }

  @BeforeMethod
  public void beforeMethod() {
    when(storageLocal.checkForRecordValidity((OPhysicalPosition) any())).thenReturn(true);

    final ODataLocal dataLocalMock = mock(ODataLocal.class);
    when(storageLocal.getDataSegmentById(anyInt())).thenReturn(dataLocalMock);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    extendibleHashingCluster.truncate();
  }

  public void testKeyPut() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

      Assert.assertTrue(extendibleHashingCluster.addPhysicalPosition(position), "i " + i);
      Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

      Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), i + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

      Assert.assertNull(extendibleHashingCluster.getPhysicalPosition(position));
    }
  }

  public void testKeyPutRandomUniform() throws IOException {
    final List<ONodeId> keys = new ArrayList<ONodeId>();

    while (keys.size() < KEYS_COUNT) {
      ONodeId key = ONodeId.generateUniqueId();

      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(key);
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "key " + key);
      }
    }

    for (ONodeId key : keys) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "" + key);
    }
  }

  public void testKeyPutRandomGaussian() throws IOException {
    List<ONodeId> keys = new ArrayList<ONodeId>();
    MersenneTwisterFast random = new MersenneTwisterFast();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      long key = (long) (random.nextGaussian() * Long.MAX_VALUE / 2 + Long.MAX_VALUE);
      if (key < 0)
        continue;
      final ONodeId nodeId = ONodeId.valueOf(key).shiftLeft(128);

      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(nodeId);
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "key " + key);
      }
    }

    for (ONodeId key : keys) {
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "" + key);
    }
  }

  public void testKeyDelete() throws IOException {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

      extendibleHashingCluster.addPhysicalPosition(position);
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

        extendibleHashingCluster.removePhysicalPosition(position.clusterPosition);
      }
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

        Assert.assertNull(extendibleHashingCluster.getPhysicalPosition(position));
      } else {
        final ONodeId nodeId = ONodeId.valueOf(i).shiftLeft(128);
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));

        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position));
      }
    }
  }

  public void testKeyDeleteRandomUniform() throws IOException {
    HashSet<ONodeId> keys = new HashSet<ONodeId>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      ONodeId key = ONodeId.generateUniqueId();
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));

      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(key);
      }
    }

    for (ONodeId key : keys) {
      if (key.longValueHigh() % 3 == 0) {
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
        extendibleHashingCluster.removePhysicalPosition(position.clusterPosition);
      }
    }

    for (ONodeId key : keys) {
      if (key.longValueHigh() % 3 == 0) {
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
        Assert.assertNull(extendibleHashingCluster.getPhysicalPosition(position));
      } else {
        final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws IOException {
    HashSet<ONodeId> nodeIds = new HashSet<ONodeId>();

    MersenneTwisterFast random = new MersenneTwisterFast();
    while (nodeIds.size() < KEYS_COUNT) {
      long key = (long) (random.nextGaussian() * Long.MAX_VALUE / 2 + Long.MAX_VALUE);
      if (key < 0)
        continue;

      final ONodeId nodeId = ONodeId.valueOf(key).shiftLeft(128);
      final OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        nodeIds.add(nodeId);
      }
    }

    for (ONodeId nodeId : nodeIds) {
      if (nodeId.longValueHigh() % 3 == 0) {
        final OClusterPosition position = new OClusterPositionNodeId(nodeId);
        extendibleHashingCluster.removePhysicalPosition(position);
      }
    }

    for (ONodeId nodeId : nodeIds) {
      if (nodeId.longValueHigh() % 3 == 0) {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));
        Assert.assertNull(extendibleHashingCluster.getPhysicalPosition(position));
      } else {
        OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(nodeId));
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position));
      }
    }
  }

  public void testNextHaveRightOrder() throws Exception {
    List<ONodeId> keys = new ArrayList<ONodeId>();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      ONodeId key = ONodeId.generateUniqueId();

      OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(key);
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "key " + key);
      }
    }

    Collections.sort(keys);

    OPhysicalPosition[] positions = extendibleHashingCluster.ceilingPositions(new OPhysicalPosition(new OClusterPositionNodeId(
        ONodeId.ZERO)));
    int curPos = 0;
    for (ONodeId key : keys) {
      OClusterPosition lhKey = positions[curPos].clusterPosition;

      Assert.assertEquals(new OClusterPositionNodeId(key), lhKey, "" + key);
      curPos++;
      if (curPos >= positions.length) {
        positions = extendibleHashingCluster.higherPositions(positions[positions.length - 1]);
        curPos = 0;
      }
    }
  }

  public void testNextSkipsRecordValid() throws Exception {
    List<ONodeId> keys = new ArrayList<ONodeId>();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      ONodeId key = ONodeId.generateUniqueId();

      OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(key);
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "key " + key);
      }
    }

    Collections.sort(keys);

    OPhysicalPosition[] positions = extendibleHashingCluster.ceilingPositions(new OPhysicalPosition(new OClusterPositionNodeId(keys
        .get(10))));
    int curPos = 0;
    for (ONodeId key : keys) {
      if (key.compareTo(keys.get(10)) < 0) {
        continue;
      }
      OClusterPosition lhKey = positions[curPos].clusterPosition;
      Assert.assertEquals(new OClusterPositionNodeId(key), lhKey, "" + key);

      curPos++;
      if (curPos >= positions.length) {
        positions = extendibleHashingCluster.higherPositions(positions[positions.length - 1]);
        curPos = 0;
      }
    }
  }

  public void testNextHaveRightOrderUsingNextMethod() throws Exception {
    List<ONodeId> keys = new ArrayList<ONodeId>();
    keys.clear();

    while (keys.size() < KEYS_COUNT) {
      ONodeId key = ONodeId.generateUniqueId();

      OPhysicalPosition position = new OPhysicalPosition(new OClusterPositionNodeId(key));
      if (extendibleHashingCluster.addPhysicalPosition(position)) {
        keys.add(key);
        Assert.assertNotNull(extendibleHashingCluster.getPhysicalPosition(position), "key " + key);
      }
    }

    Collections.sort(keys);

    // test finding is unsuccessful
    for (ONodeId key : keys) {
      OClusterPosition lhKey = extendibleHashingCluster.ceilingPositions(new OPhysicalPosition(new OClusterPositionNodeId(key)))[0].clusterPosition;
      Assert.assertEquals(new OClusterPositionNodeId(key), lhKey, "" + key);
    }

    // test finding is successful
    for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
      ONodeId key = keys.get(j);
      OClusterPosition lhKey = extendibleHashingCluster.higherPositions(new OPhysicalPosition(new OClusterPositionNodeId(key)))[0].clusterPosition;
      Assert.assertEquals(new OClusterPositionNodeId(keys.get(j + 1)), lhKey, "" + j);
    }
  }
}
