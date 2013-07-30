/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.collection;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import com.orientechnologies.common.util.OResettable;

/**
 * Base class for OMVRBTree Iterators
 */
public abstract class AbstractEntryIterator<K, V, T> implements OLazyIterator<T>, OResettable {
  OMVRBTree<K, V>      tree;
  OMVRBTreeEntry<K, V> begin;
  OMVRBTreeEntry<K, V> next;
  OMVRBTreeEntry<K, V> lastReturned;
  int                  expectedModCount;
  int                  pageIndex;

  AbstractEntryIterator(final OMVRBTreeEntry<K, V> start) {
    begin = start;
    init();
  }

  private void init() {
    if (begin == null)
      // IN CASE OF ABSTRACTMAP.HASHCODE()
      return;

    tree = begin.getTree();
    next = begin;
    expectedModCount = tree.modCount;
    lastReturned = null;
    pageIndex = begin.getTree().getPageIndex() > -1 ? begin.getTree().getPageIndex() - 1 : -1;
  }

  @Override
  public void reset() {
    init();
  }

  public boolean hasNext() {
    if (tree != null && expectedModCount != tree.modCount) {
      // CONCURRENT CHANGE: TRY TO REUSE LAST POSITION
      pageIndex--;
      expectedModCount = tree.modCount;
    }

    return next != null && (pageIndex < next.getSize() - 1 || OMVRBTree.successor(next) != null);
  }

  public final boolean hasPrevious() {
    if (tree != null && expectedModCount != tree.modCount) {
      // CONCURRENT CHANGE: TRY TO REUSE LAST POSITION
      pageIndex = -1;
      expectedModCount = tree.modCount;
    }

    return next != null && (pageIndex > 0 || OMVRBTree.predecessor(next) != null);
  }

  final K nextKey() {
    return nextEntry().getKey(pageIndex);
  }

  final V nextValue() {
    return nextEntry().getValue(pageIndex);
  }

  final V prevValue() {
    return prevEntry().getValue(pageIndex);
  }

  final OMVRBTreeEntry<K, V> nextEntry() {
    if (next == null)
      throw new NoSuchElementException();

    if (pageIndex < next.getSize() - 1) {
      // ITERATE INSIDE THE NODE
      pageIndex++;
    } else {
      // GET THE NEXT NODE
      if (tree.modCount != expectedModCount)
        throw new ConcurrentModificationException();

      next = OMVRBTree.successor(next);
      pageIndex = 0;
    }
    lastReturned = next;
    tree.pageIndex = pageIndex;

    return next;
  }

  final OMVRBTreeEntry<K, V> prevEntry() {
    if (next == null)
      throw new NoSuchElementException();

    if (pageIndex > 0) {
      // ITERATE INSIDE THE NODE
      pageIndex--;
    } else {
      if (tree.modCount != expectedModCount)
        throw new ConcurrentModificationException();

      next = OMVRBTree.predecessor(next);
      pageIndex = next != null ? next.getSize() - 1 : -1;
    }

    lastReturned = next;
    return next;
  }

  @SuppressWarnings("unchecked")
  public T update(final T iValue) {
    if (lastReturned == null)
      throw new IllegalStateException();
    if (tree.modCount != expectedModCount)
      throw new ConcurrentModificationException();
    tree.pageIndex = pageIndex;
    return (T) next.setValue((V) iValue);
  }

  public void remove() {
    if (lastReturned == null)
      throw new IllegalStateException();
    if (tree.modCount != expectedModCount)
      throw new ConcurrentModificationException();
    // deleted entries are replaced by their successors
    if (lastReturned.getLeft() != null && lastReturned.getRight() != null)
      next = lastReturned;
    tree.pageIndex = pageIndex;
    next = tree.deleteEntry(lastReturned);
    pageIndex--;
    expectedModCount = tree.modCount;
    lastReturned = null;
  }
}
