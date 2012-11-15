package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * @author Artem Loginov
 */
public class ClusterLocalLHPEPSTest {
  private static final String PATH_TO_FILE = "target/databases/123";
  private static final String FILE_NAME    = "name";
  private static final int    KEYS_COUNT   = 100000;

  private OCluster getCluster() throws IOException {

    OStorage storage = new OStorageLocal(FILE_NAME + System.currentTimeMillis(), PATH_TO_FILE + System.currentTimeMillis(), "rw");
    storage.create(null);
    Collection<? extends OCluster> clusterInstances = storage.getClusterInstances();
    OCluster cluster = null;
    for (OCluster clusterInstance : clusterInstances) {
      System.out.println(clusterInstance.getClass().getCanonicalName() + " : " + clusterInstance.getName());
      Assert.assertTrue(clusterInstance instanceof OClusterLocalLHPEPS);
      if ("default".equals(clusterInstance.getName()))
        cluster = clusterInstance;
    }

    Assert.assertNotNull(cluster);

    return cluster;
  }

  @BeforeClass
  public void setUp() {
    OGlobalConfiguration.USE_LHPEPS_CLUSTER.setValue(true);
  }

  @AfterClass
  public void tearDown() {
    OGlobalConfiguration.USE_LHPEPS_CLUSTER.setValue(false);
  }

  @Test(enabled = false)
  public void testKeyPut() throws IOException {
    OCluster localCluster = getCluster();

    for (int i = 0; i < KEYS_COUNT; i++) {
      localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i)));
      Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))), i
          + " key is absent");
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));
    }
  }

  @Test(enabled = false)
  public void testKeyPutRandom() throws IOException {
    OCluster localCluster = getCluster();

    List<Long> keys = new ArrayList<Long>();
    Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      long key = random.nextLong();
      if (key < 0)
        key = -key;

      if (localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key)))) {
        keys.add(key);
        Assert
            .assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key))));
      }
    }

    for (long key : keys)
      Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key))));
  }

  @Test(enabled = false)
  public void testKeyDelete() throws IOException {
    OCluster localCluster = getCluster();

    for (int i = 0; i < KEYS_COUNT; i++)
      localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i)));

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        localCluster.removePhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i));
      // Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(i)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));
      else
        Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));
    }
  }

  @Test(enabled = false)
  public void testKeyDeleteRandom() throws IOException {
    OCluster localCluster = getCluster();
    Set<Long> longs = new HashSet<Long>();
    final Random random = new Random();

    for (int i = 0; i < KEYS_COUNT; i++) {
      long key = random.nextLong();
      if (key < 0)
        key = -key;

      localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key)));
      longs.add(key);
    }

    for (long key : longs) {
      if (key % 3 == 0) {
        localCluster.removePhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key));
      }
    }

    for (long key : longs) {
      if (key % 3 == 0)
        Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key))));
      else
        Assert
            .assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(key))));
    }
  }

  @Test(enabled = false)
  public void testKeyAddDelete() throws IOException {
    OCluster localCluster = getCluster();

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertNotNull(localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        localCluster.removePhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i));

      if (i % 2 == 0)
        Assert.assertNotNull(localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE
            .valueOf(KEYS_COUNT + i))));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));
      else
        Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(i))));

      if (i % 2 == 0)
        Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE
            .valueOf(KEYS_COUNT + i))));
    }
  }

  @Test(enabled = false)
  public void testKeyAddDeleteRandom() throws IOException {
    OCluster localCluster = getCluster();
    List<Long> longs = getUniqueRandomValuesArray(2 * KEYS_COUNT);

    // add
    for (int i = 0; i < KEYS_COUNT; i++) {
      localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs.get(i))));
    }

    // remove+add
    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        localCluster.removePhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs.get(i)));
      }

      if (i % 2 == 0) {
        localCluster
            .addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs.get(i + KEYS_COUNT))));
      }
    }

    // check removed ok
    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0)
        Assert.assertNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs
            .get(i)))));
      else
        Assert.assertNotNull(localCluster.addPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs
            .get(i)))));

      if (i % 2 == 0)
        Assert.assertNotNull(localCluster.getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(longs
            .get(KEYS_COUNT + i)))));
    }
  }

  private List<Long> getUniqueRandomValuesArray(int size) {
    Random random = new Random();
    long data[] = new long[size];
    for (int i = 0, dataLength = data.length; i < dataLength; i++) {
      data[i] = i * 5 + random.nextInt(5);
    }

    int max = data.length - 1;

    List<Long> list = new ArrayList<Long>(size);
    while (max > 0) {

      swap(data, max, Math.abs(random.nextInt(max)));
      list.add(data[max--]);
    }
    return list;
  }

  private void swap(long[] data, int firstIndex, int secondIndex) {
    long tmp = data[firstIndex];
    data[firstIndex] = data[secondIndex];
    data[secondIndex] = tmp;
  }
}
