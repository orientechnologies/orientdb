package com.orientechnologies.orient.core.index.sbtree.local;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.*;

import com.orientechnologies.orient.core.id.ORID;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.index.OCompositeKey;
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

    localSBTree = new OSBTree<OCompositeKey, OIdentifiable>(".sbt", 2, false, ".nbt");
    localSBTree.create("localSBTreeCompositeKeyTest", OCompositeKeySerializer.INSTANCE, OLinkSerializer.INSTANCE, null,
        (OStorageLocalAbstract) databaseDocumentTx.getStorage().getUnderlying(), false);
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

  public void testIterateBetweenValuesInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true,
        compositeKey(3.0), true, true);

    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong(j))));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);
    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong(j))));
      }
    }
  }

  public void testIterateBetweenValuesFromInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong(j))));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong(j))));
    }
  }

  public void testIterateBetweenValuesToInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false,
        compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }
  }

  public void testIterateEntriesNonInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false,
        compositeKey(3.0), false, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 0);

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 0);

    cursor = localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, true);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }
  }

  public void testIterateBetweenValuesInclusivePartialKey() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true,
        compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }
  }

  public void testIterateBetweenValuesFromInclusivePartialKey() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((j)))));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((j)))));
    }
  }

  public void testIterateBetweenValuesToInclusivePartialKey() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false,
        compositeKey(3.0), true, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), true, false);
    orids = extractRids(cursor);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
    }

  }

  public void testIterateBetweenValuesNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, new OClusterPositionLong((i)))));
    }
  }

  public void testIterateValuesMajorInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testIterateMajorNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, new OClusterPositionLong((i)))));
    }
  }

  public void testIterateValuesMajorInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree
        .iterateEntriesMajor(compositeKey(2.0, 3.0), true, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testIterateValuesMajorNonInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false,
        true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testIterateValuesMinorInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

  }

  public void testIterateValuesMinorNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testIterateValuesMinorInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree
        .iterateEntriesMinor(compositeKey(3.0, 2.0), true, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2)
          continue;

        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2)
          continue;

        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  public void testIterateValuesMinorNonInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false,
        true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, new OClusterPositionLong((j)))));
      }
  }

  private OCompositeKey compositeKey(Comparable<?>... params) {
    return new OCompositeKey(Arrays.asList(params));
  }

  private Set<ORID> extractRids(OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor) {
    final Set<ORID> orids = new HashSet<ORID>();
    while (true) {
      Map.Entry<OCompositeKey, OIdentifiable> entry = cursor.next(-1);
      if (entry != null)
        orids.add(entry.getValue().getIdentity());
      else
        break;
    }
    return orids;
  }

}
