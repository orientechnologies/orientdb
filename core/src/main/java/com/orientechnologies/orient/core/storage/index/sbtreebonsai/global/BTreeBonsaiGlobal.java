package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

public class BTreeBonsaiGlobal implements OSBTreeBonsai<OIdentifiable, Integer> {

  private final BTree bTree;
  private final int intFileId;
  private final long ridBagId;

  private final OBinarySerializer<OIdentifiable> keySerializer;
  private final OBinarySerializer<Integer> valueSerializer;

  public BTreeBonsaiGlobal(
      final BTree bTree,
      final int intFileId,
      final long ridBagId,
      OBinarySerializer<OIdentifiable> keySerializer,
      OBinarySerializer<Integer> valueSerializer) {
    this.bTree = bTree;
    this.intFileId = intFileId;
    this.ridBagId = ridBagId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public OBonsaiCollectionPointer getCollectionPointer() {
    return new OBonsaiCollectionPointer(intFileId, getRootBucketPointer());
  }

  @Override
  public long getFileId() {
    return bTree.getFileId();
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    return new OBonsaiBucketPointer(ridBagId, 0);
  }

  @Override
  public Integer get(OIdentifiable key) {
    final ORID rid = key.getIdentity();

    final int result;

    result = bTree.get(new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()));

    if (result < 0) {
      return null;
    }

    return result;
  }

  @Override
  public boolean put(OAtomicOperation atomicOperation, OIdentifiable key, Integer value) {
    final ORID rid = key.getIdentity();

    return bTree.put(
        atomicOperation,
        new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
        value);
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    try (Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<ORawPair<EdgeKey, Integer>> iterator = stream.iterator();

      while (iterator.hasNext()) {
        final ORawPair<EdgeKey, Integer> entry = iterator.next();
        bTree.remove(atomicOperation, entry.first);
      }
    }
  }

  public boolean isEmpty() {
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesMajor(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE), true, true)) {
      return !stream.findAny().isPresent();
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    clear(atomicOperation);
  }

  @Override
  public Integer remove(OAtomicOperation atomicOperation, OIdentifiable key) {
    final ORID rid = key.getIdentity();
    final int result;
    result =
        bTree.remove(
            atomicOperation, new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()));

    if (result < 0) {
      return null;
    }

    return result;
  }

  @Override
  public Collection<Integer> getValuesMinor(
      OIdentifiable key, boolean inclusive, int maxValuesToFetch) {
    final ORID rid = key.getIdentity();
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {

      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public void loadEntriesMinor(
      OIdentifiable key, boolean inclusive, RangeResultListener<OIdentifiable, Integer> listener) {
    final ORID rid = key.getIdentity();
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public Collection<Integer> getValuesMajor(
      OIdentifiable key, boolean inclusive, int maxValuesToFetch) {
    final ORID rid = key.getIdentity();

    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public void loadEntriesMajor(
      OIdentifiable key,
      boolean inclusive,
      boolean ascSortOrder,
      RangeResultListener<OIdentifiable, Integer> listener) {
    final ORID rid = key.getIdentity();
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public Collection<Integer> getValuesBetween(
      OIdentifiable keyFrom,
      boolean fromInclusive,
      OIdentifiable keyTo,
      boolean toInclusive,
      int maxValuesToFetch) {
    final ORID ridFrom = keyFrom.getIdentity();
    final ORID ridTo = keyTo.getIdentity();
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, ridFrom.getClusterId(), ridFrom.getClusterPosition()),
            fromInclusive,
            new EdgeKey(ridBagId, ridTo.getClusterId(), ridTo.getClusterPosition()),
            toInclusive,
            true)) {
      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public OIdentifiable firstKey() {
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<ORawPair<EdgeKey, Integer>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final ORawPair<EdgeKey, Integer> entry = iterator.next();
        return new ORecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public OIdentifiable lastKey() {
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      final Iterator<ORawPair<EdgeKey, Integer>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final ORawPair<EdgeKey, Integer> entry = iterator.next();
        return new ORecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public void loadEntriesBetween(
      OIdentifiable keyFrom,
      boolean fromInclusive,
      OIdentifiable keyTo,
      boolean toInclusive,
      RangeResultListener<OIdentifiable, Integer> listener) {
    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public int getRealBagSize(Map<OIdentifiable, Change> changes) {
    final Map<OIdentifiable, Change> notAppliedChanges = new HashMap<>(changes);
    final OModifiableInteger size = new OModifiableInteger(0);

    try (final Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      forEachEntry(
          stream,
          entry -> {
            final ORecordId rid =
                new ORecordId(entry.first.targetCluster, entry.first.targetPosition);
            final Change change = notAppliedChanges.remove(rid);
            final int result;

            final Integer treeValue = entry.second;
            if (change == null) {
              result = treeValue;
            } else {
              result = change.applyTo(treeValue);
            }

            size.increment(result);
            return true;
          });
    }

    for (final Change change : notAppliedChanges.values()) {
      final int result = change.applyTo(0);
      size.increment(result);
    }

    return size.value;
  }

  @Override
  public OBinarySerializer<OIdentifiable> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public OBinarySerializer<Integer> getValueSerializer() {
    return valueSerializer;
  }

  private static void forEachEntry(
      final Stream<ORawPair<EdgeKey, Integer>> stream,
      final Function<ORawPair<EdgeKey, Integer>, Boolean> consumer) {

    boolean cont = true;

    final Iterator<ORawPair<EdgeKey, Integer>> iterator = stream.iterator();
    while (iterator.hasNext() && cont) {
      cont = consumer.apply(iterator.next());
    }
  }

  private static List<Integer> streamToList(
      final Stream<ORawPair<EdgeKey, Integer>> stream, int maxValuesToFetch) {
    if (maxValuesToFetch < 0) {
      maxValuesToFetch = Integer.MAX_VALUE;
    }

    final ArrayList<Integer> result = new ArrayList<>(Math.max(8, maxValuesToFetch));

    final int limit = maxValuesToFetch;
    forEachEntry(
        stream,
        entry -> {
          result.add(entry.second);
          return result.size() < limit;
        });

    return result;
  }

  private static void listenStream(
      final Stream<ORawPair<EdgeKey, Integer>> stream,
      final RangeResultListener<OIdentifiable, Integer> listener) {
    forEachEntry(
        stream,
        entry ->
            listener.addResult(
                new Entry<OIdentifiable, Integer>() {
                  @Override
                  public OIdentifiable getKey() {
                    return new ORecordId(entry.first.targetCluster, entry.first.targetPosition);
                  }

                  @Override
                  public Integer getValue() {
                    return entry.second;
                  }

                  @Override
                  public Integer setValue(Integer value) {
                    throw new UnsupportedOperationException();
                  }
                }));
  }
}
