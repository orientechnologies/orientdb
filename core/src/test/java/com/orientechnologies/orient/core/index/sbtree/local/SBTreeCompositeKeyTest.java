package com.orientechnologies.orient.core.index.sbtree.local;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;

/**
 * @author Andrey Lomakin
 * @since 15.08.13
 */
@Test
public class SBTreeCompositeKeyTest extends DatabaseAbstractTest {

  private OSBTree<OCompositeKey, OIdentifiable> localSBTree;

  @BeforeClass
  public void beforeClass() {
    super.beforeClass();

    localSBTree = new OSBTree<OCompositeKey, OIdentifiable>(".sbt", false, ".nbt", (OAbstractPaginatedStorage) database
        .getStorage().getUnderlying());
    localSBTree.create("localSBTreeCompositeKeyTest", OCompositeKeySerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 2, false);
  }

  @BeforeMethod
  public void beforeMethod() {
    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        localSBTree.put(compositeKey, new ORecordId((int) i, (long) j));
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

    super.afterClass();
  }

  public void testIterateBetweenValuesInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true,
        compositeKey(3.0), true, true);

    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);
    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }
  }

  public void testIterateBetweenValuesFromInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }
  }

  public void testIterateBetweenValuesToInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false,
        compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
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
      assertTrue(orids.contains(new ORecordId(2, i)));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
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
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }
  }

  public void testIterateBetweenValuesFromInclusivePartialKey() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
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
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), true, false);
    orids = extractRids(cursor);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4)
          continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

  }

  public void testIterateBetweenValuesNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false,
        compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }

    cursor = localSBTree.iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }
  }

  public void testIterateValuesMajorInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  public void testIterateMajorNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
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
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
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
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3)
          continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  public void testIterateValuesMinorInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

  }

  public void testIterateValuesMinorNonInclusivePartial() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
    Set<ORID> orids = extractRids(cursor);
    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
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

        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2)
          continue;

        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  public void testIterateValuesMinorNonInclusive() {
    OSBTree.OSBTreeCursor<OCompositeKey, OIdentifiable> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false,
        true);
    Set<ORID> orids = extractRids(cursor);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, false);
    orids = extractRids(cursor);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
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
