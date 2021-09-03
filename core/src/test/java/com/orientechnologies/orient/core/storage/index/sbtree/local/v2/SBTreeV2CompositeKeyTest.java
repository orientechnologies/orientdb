package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 15.08.13
 */
public class SBTreeV2CompositeKeyTest extends DatabaseAbstractTest {

  private OSBTreeV2<OCompositeKey, OIdentifiable> localSBTree;
  private OAtomicOperationsManager atomicOperationsManager;

  @Before
  public void beforeMethod() throws Exception {
    atomicOperationsManager =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) database).getStorage())
            .getAtomicOperationsManager();
    //noinspection deprecation
    localSBTree =
        new OSBTreeV2<>(
            "localSBTreeCompositeKeyTest",
            ".sbt",
            ".nbt",
            (OAbstractPaginatedStorage) database.getStorage());
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localSBTree.create(
                atomicOperation,
                OCompositeKeySerializer.INSTANCE,
                OLinkSerializer.INSTANCE,
                null,
                2,
                false,
                null));

    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                localSBTree.put(
                    atomicOperation,
                    compositeKey,
                    new ORecordId(
                        ((Double) compositeKey.getKeys().get(0)).intValue(),
                        ((Double) compositeKey.getKeys().get(1)).longValue())));
      }
    }
  }

  @After
  public void afterClass() throws Exception {
    try (Stream<OCompositeKey> keyStream = localSBTree.keyStream()) {
      keyStream.forEach(
          (key) -> {
            try {
              atomicOperationsManager.executeInsideAtomicOperation(
                  null, atomicOperation -> localSBTree.remove(atomicOperation, key));
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          });
    }

    if (localSBTree.isNullPointerSupport()) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localSBTree.remove(atomicOperation, null));
    }

    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> localSBTree.delete(atomicOperation));
  }

  @Test
  public void testIterateBetweenValuesInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, true);

    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);
    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }
  }

  @Test
  public void testIterateEntriesNonInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), false, true);
    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 0);

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 0);

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, true);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(1.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }
  }

  @Test
  public void testIterateBetweenValuesInclusivePartialKey() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusivePartialKey() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new ORecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusivePartialKey() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), true, true);

    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), true, false);
    orids = extractRids(stream);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesNonInclusivePartial() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, true);

    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(2, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusivePartial() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateMajorNonInclusivePartial() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new ORecordId(3, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateValuesMajorNonInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) continue;
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateValuesMinorInclusivePartial() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateValuesMinorNonInclusivePartial() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateValuesMinorInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, true);
    Set<ORID> orids = extractRids(stream);
    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) continue;

        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++)
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) continue;

        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  @Test
  public void testIterateValuesMinorNonInclusive() {
    Stream<ORawPair<OCompositeKey, OIdentifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, true);
    Set<ORID> orids = extractRids(stream);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++)
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new ORecordId(i, j)));
      }
  }

  private static OCompositeKey compositeKey(Comparable<?>... params) {
    return new OCompositeKey(Arrays.asList(params));
  }

  private static Set<ORID> extractRids(Stream<ORawPair<OCompositeKey, OIdentifiable>> stream) {
    return stream.map((entry) -> entry.second.getIdentity()).collect(Collectors.toSet());
  }
}
