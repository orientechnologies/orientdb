package com.orientechnologies.orient.core.index.sbtree.local;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.testng.annotations.*;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 15.08.13
 */
@Test
public class SBTreeCompositeKeyTest {
  private ODatabaseDocumentTx                   databaseDocumentTx;

  private OSBTree<OCompositeKey, OIdentifiable> localSBTree;
  private String                                buildDirectory;

  @BeforeClass
  public void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localSBTreeCompositeKeyTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    localSBTree = new OSBTree<OCompositeKey, OIdentifiable>(".sbt", 2, false);
    localSBTree.create("localSBTreeCompositeKeyTest", OCompositeKeySerializer.INSTANCE, OLinkSerializer.INSTANCE, null,
        (OStorageLocalAbstract) databaseDocumentTx.getStorage().getUnderlying());
  }

  @BeforeMethod
  public void beforeMethod() {
    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        localSBTree.put(compositeKey, new ORecordId((int) i, new OClusterPositionLong((long) (j))));
      }
    }
  }

  @AfterMethod
  public void afterMethod() {
    localSBTree.clear();
  }

  @AfterClass
  public void afterClass() throws Exception {
    localSBTree.clear();
    localSBTree.delete();
    databaseDocumentTx.drop();
  }

  public void testBetweenValuesInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0), true, compositeKey(3.0), true, -1);
    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong(j))));
      }
    }
  }

  public void testBetweenValuesFromInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0), true, compositeKey(3.0), false, -1);

    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong(j))));
    }
  }

  public void testBetweenValuesToInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0), false, compositeKey(3.0), true, -1);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }
  }

  public void testBetweenValuesNonInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0), false, compositeKey(3.0), false, -1);

    assertEquals(orids.size(), 0);

    orids = localSBTree.getValuesBetween(compositeKey(1.0), false, compositeKey(3.0), false, -1);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }
  }

  public void testBetweenValuesInclusivePartialKey() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), true, -1);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }
  }

  public void testBetweenValuesFromInclusivePartialKey() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), false, -1);

    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((j)))));
    }
  }

  public void testBetweenValuesToInclusivePartialKey() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), true, -1);

    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }
  }

  @Test
  public void testBetweenValuesNonInclusivePartial() {
    Collection<OIdentifiable> orids = localSBTree.getValuesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), false, -1);
    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }
  }

  public void testValuesMajorInclusivePartial() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMajor(compositeKey(2.0), true, -1);
    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  @Test
  public void testValuesMajorNonInclusivePartial() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMajor(compositeKey(2.0), false, -1);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }
  }

  public void testValuesMajorInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMajor(compositeKey(2.0, 3.0), true, -1);

    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testValuesMajorNonInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMajor(compositeKey(2.0, 3.0), false, -1);
    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testValuesMinorInclusivePartial() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMinor(compositeKey(3.0), true, -1);
    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testValuesMinorNonInclusivePartial() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMinor(compositeKey(3.0), false, -1);
    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testValuesMinorInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMinor(compositeKey(3.0, 2.0), true, -1);
    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2)
          continue;

        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testValuesMinorNonInclusive() {
    Collection<OIdentifiable> orids = localSBTree.getValuesMinor(compositeKey(3.0, 2.0), false, -1);
    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  private OCompositeKey compositeKey(Comparable<?>... params) {
    return new OCompositeKey(Arrays.asList(params));
  }

}
