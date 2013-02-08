package com.orientechnologies.orient.core.storage.impl.local.eh;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.OClusterPositionNodeId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

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
    extendibleHashingCluster = new OClusterLocalEH(46, 25, new OClusterPositionNodeId(ONodeId.ZERO),
        new OClusterPositionFactory.OClusterPositionFactoryNodeId());

    MockitoAnnotations.initMocks(this);
    when(storageLocal.getMode()).thenReturn("rw");
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

}
