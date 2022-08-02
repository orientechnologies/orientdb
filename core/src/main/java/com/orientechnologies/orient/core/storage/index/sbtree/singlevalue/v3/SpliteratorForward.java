package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

final class SpliteratorForward<K> implements Spliterator<ORawPair<K, ORID>> {
  /** */
  private final CellBTreeSingleValueV3<K> btree;

  private final K fromKey;
  private final K toKey;
  private final boolean fromKeyInclusive;
  private final boolean toKeyInclusive;

  private int pageIndex = -1;
  private int itemIndex = -1;

  private OLogSequenceNumber lastLSN = null;

  private final List<ORawPair<K, ORID>> dataCache = new ArrayList<>();
  private Iterator<ORawPair<K, ORID>> cacheIterator = Collections.emptyIterator();

  SpliteratorForward(
      CellBTreeSingleValueV3<K> cellBTreeSingleValueV3,
      final K fromKey,
      final K toKey,
      final boolean fromKeyInclusive,
      final boolean toKeyInclusive) {
    btree = cellBTreeSingleValueV3;
    this.fromKey = fromKey;
    this.toKey = toKey;

    this.toKeyInclusive = toKeyInclusive;
    this.fromKeyInclusive = fromKeyInclusive;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<K, ORID>> action) {
    if (cacheIterator == null) {
      return false;
    }

    if (cacheIterator.hasNext()) {
      action.accept(cacheIterator.next());
      return true;
    }

    btree.fetchNextForwardCachePortion(this);

    cacheIterator = getDataCache().iterator();

    if (cacheIterator.hasNext()) {
      action.accept(cacheIterator.next());
      return true;
    }

    cacheIterator = null;

    return false;
  }

  @Override
  public Spliterator<ORawPair<K, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return SORTED | NONNULL | ORDERED;
  }

  @Override
  public Comparator<? super ORawPair<K, ORID>> getComparator() {
    return (pairOne, pairTwo) -> btree.comparator.compare(pairOne.first, pairTwo.first);
  }

  public K getFromKey() {
    return fromKey;
  }

  public K getToKey() {
    return toKey;
  }

  public boolean isFromKeyInclusive() {
    return fromKeyInclusive;
  }

  public boolean isToKeyInclusive() {
    return toKeyInclusive;
  }

  public int getItemIndex() {
    return itemIndex;
  }

  public void setItemIndex(int itemIndex) {
    this.itemIndex = itemIndex;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public void setPageIndex(int pageIndex) {
    this.pageIndex = pageIndex;
  }

  List<ORawPair<K, ORID>> getDataCache() {
    return dataCache;
  }

  OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  void setLastLSN(OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }

  public void setCacheIterator(Iterator<ORawPair<K, ORID>> cacheIterator) {
    this.cacheIterator = cacheIterator;
  }
}
