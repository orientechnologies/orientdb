package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public final class SpliteratorBackward implements Spliterator<ORawPair<EdgeKey, Integer>> {

  /** */
  private final BTree bTree;

  private final EdgeKey fromKey;
  private final EdgeKey toKey;
  private final boolean fromKeyInclusive;
  private final boolean toKeyInclusive;

  private int pageIndex = -1;
  private int itemIndex = -1;

  private OLogSequenceNumber lastLSN = null;

  private final List<ORawPair<EdgeKey, Integer>> dataCache = new ArrayList<>();
  private Iterator<ORawPair<EdgeKey, Integer>> cacheIterator = Collections.emptyIterator();

  SpliteratorBackward(
      BTree bTree,
      final EdgeKey fromKey,
      final EdgeKey toKey,
      final boolean fromKeyInclusive,
      final boolean toKeyInclusive) {
    this.bTree = bTree;
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.fromKeyInclusive = fromKeyInclusive;
    this.toKeyInclusive = toKeyInclusive;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<EdgeKey, Integer>> action) {
    if (cacheIterator == null) {
      return false;
    }

    if (cacheIterator.hasNext()) {
      action.accept(cacheIterator.next());
      return true;
    }

    this.bTree.fetchNextCachePortionBackward(this);

    cacheIterator = dataCache.iterator();

    if (cacheIterator.hasNext()) {
      action.accept(cacheIterator.next());
      return true;
    }

    cacheIterator = null;

    return false;
  }

  public void clearCache() {
    getDataCache().clear();
    cacheIterator = Collections.emptyIterator();
  }

  @Override
  public Spliterator<ORawPair<EdgeKey, Integer>> trySplit() {
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
  public Comparator<? super ORawPair<EdgeKey, Integer>> getComparator() {
    return (pairOne, pairTwo) -> -pairOne.first.compareTo(pairTwo.first);
  }

  public List<ORawPair<EdgeKey, Integer>> getDataCache() {
    return dataCache;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public void setPageIndex(int pageIndex) {
    this.pageIndex = pageIndex;
  }

  public int getItemIndex() {
    return itemIndex;
  }

  public void setItemIndex(int itemIndex) {
    this.itemIndex = itemIndex;
  }

  EdgeKey getFromKey() {
    return fromKey;
  }

  EdgeKey getToKey() {
    return toKey;
  }

  boolean isFromKeyInclusive() {
    return fromKeyInclusive;
  }

  boolean isToKeyInclusive() {
    return toKeyInclusive;
  }

  void decItemIndex() {
    this.itemIndex--;
  }

  OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  void setLastLSN(OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }
}
