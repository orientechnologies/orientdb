package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 15.08.13
 */
public class SBTreeV2CompositeKeyTest extends DatabaseAbstractTest {

  private OSBTreeV2<OCompositeKey, OIdentifiable> localSBTree;

  @Before
  public void beforeMethod() throws Exception {
    //noinspection deprecation
    localSBTree = new OSBTreeV2<>("localSBTreeCompositeKeyTest", ".sbt", ".nbt",
        (OAbstractPaginatedStorage) database.getStorage().getUnderlying());
    localSBTree.create(OCompositeKeySerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 2, false, null);

    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        localSBTree.put(compositeKey, new ORecordId((int) i, (long) j));
      }
    }
  }

  @After
  public void afterClass() throws Exception {
    final Spliterator<OCompositeKey> keyCursor = localSBTree.keySpliterator();
    StreamSupport.stream(keyCursor, false).forEach((key) -> {
      try {
        localSBTree.remove(key);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    });

    if (localSBTree.isNullPointerSupport()) {
      localSBTree.remove(null);
    }

    localSBTree.delete();

  }

  @Test
  public void testIterateBetweenValuesInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, true);

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

  @Test
  public void testIterateBetweenValuesFromInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, true);

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

  @Test
  public void testIterateBetweenValuesToInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, true);
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

  @Test
  public void testIterateEntriesNonInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), false, true);
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

  @Test
  public void testIterateBetweenValuesInclusivePartialKey() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), true, true);
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

  @Test
  public void testIterateBetweenValuesFromInclusivePartialKey() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0, 4.0), true, compositeKey(3.0), false, true);

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

  @Test
  public void testIterateBetweenValuesToInclusivePartialKey() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), true, true);

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

  @Test
  public void testIterateBetweenValuesNonInclusivePartial() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesBetween(compositeKey(2.0, 4.0), false, compositeKey(3.0), false, true);

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

  @Test
  public void testIterateValuesMajorInclusivePartial() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
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

  @Test
  public void testIterateMajorNonInclusivePartial() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
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

  @Test
  public void testIterateValuesMajorInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
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

  @Test
  public void testIterateValuesMajorNonInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesMajor(compositeKey(2.0, 3.0), false, true);
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

  @Test
  public void testIterateValuesMinorInclusivePartial() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
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

  @Test
  public void testIterateValuesMinorNonInclusivePartial() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
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

  @Test
  public void testIterateValuesMinorInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
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

  @Test
  public void testIterateValuesMinorNonInclusive() {
    Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor = localSBTree
        .iterateEntriesMinor(compositeKey(3.0, 2.0), false, true);
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

  private static OCompositeKey compositeKey(Comparable<?>... params) {
    return new OCompositeKey(Arrays.asList(params));
  }

  private static Set<ORID> extractRids(Spliterator<ORawPair<OCompositeKey, OIdentifiable>> cursor) {
    return StreamSupport.stream(cursor, false).map((entry) -> entry.second.getIdentity()).collect(Collectors.toSet());
  }

}
