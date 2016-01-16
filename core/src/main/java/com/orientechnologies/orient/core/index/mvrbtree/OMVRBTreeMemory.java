/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.index.mvrbtree;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

@SuppressWarnings("serial")
public class OMVRBTreeMemory<K, V> extends OMVRBTree<K, V> {
  /**
   * The number of entries in the tree
   */
  protected int size            = 0;
  protected int defaultPageSize = 63;

  /**
   * Memory based MVRB-Tree implementation. Constructs a new, empty tree map, using the natural ordering of its keys. All keys
   * inserted into the map must implement the {@link Comparable} interface. Furthermore, all such keys must be <i>mutually
   * comparable</i>: <tt>k1.compareTo(k2)</tt> must not throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt>
   * in the map. If the user attempts to put a key into the map that violates this constraint (for example, the user attempts to put
   * a string key into a map whose keys are integers), the <tt>put(Object key, Object value)</tt> call will throw a
   * <tt>ClassCastException</tt>.
   */
  public OMVRBTreeMemory() {
    runtimeCheckEnabled = false;
  }

  public OMVRBTreeMemory(final int iPageSize, final float iLoadFactor) {
    this(iPageSize, iLoadFactor, 1);
  }

  public OMVRBTreeMemory(final int iPageSize, final float iLoadFactor, final int keySize) {
    super(keySize);
    defaultPageSize = iPageSize;
    pageLoadFactor = iLoadFactor;
  }

  /**
   * Constructs a new, empty tree map, ordered according to the given comparator. All keys inserted into the map must be <i>mutually
   * comparable</i> by the given comparator: <tt>comparator.compare(k1,
   * k2)</tt> must not throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt> in the map. If the user attempts
   * to put a key into the map that violates this constraint, the <tt>put(Object
   * key, Object value)</tt> call will throw a <tt>ClassCastException</tt>.
   * 
   * @param comparator
   *          the comparator that will be used to order this map. If <tt>null</tt>, the {@linkplain Comparable natural ordering} of
   *          the keys will be used.
   */
  public OMVRBTreeMemory(final Comparator<? super K> comparator) {
    super(comparator);
  }

  /**
   * Constructs a new tree map containing the same mappings as the given map, ordered according to the <i>natural ordering</i> of
   * its keys. All keys inserted into the new map must implement the {@link Comparable} interface. Furthermore, all such keys must
   * be <i>mutually comparable</i>: <tt>k1.compareTo(k2)</tt> must not throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt>
   * and <tt>k2</tt> in the map. This method runs in n*log(n) time.
   * 
   * @param m
   *          the map whose mappings are to be placed in this map
   * @throws ClassCastException
   *           if the keys in m are not {@link Comparable}, or are not mutually comparable
   * @throws NullPointerException
   *           if the specified map is null
   */
  public OMVRBTreeMemory(final Map<? extends K, ? extends V> m) {
    super(m);
  }

  /**
   * Constructs a new tree map containing the same mappings and using the same ordering as the specified sorted map. This method
   * runs in linear time.
   * 
   * @param m
   *          the sorted map whose mappings are to be placed in this map, and whose comparator is to be used to sort this map
   * @throws NullPointerException
   *           if the specified map is null
   */
  public OMVRBTreeMemory(final SortedMap<K, ? extends V> m) {
    super(m);
  }

  @Override
  public int getTreeSize() {
    return size;
  }

  protected void setSize(int pSize) {
    size = pSize;
  }

  public int getDefaultPageSize() {
    return defaultPageSize;
  }

  @Override
  protected OMVRBTreeEntry<K, V> createEntry(final K key, final V value) {
    return new OMVRBTreeEntryMemory<K, V>(this, key, value, null);
  }

  @Override
  protected OMVRBTreeEntry<K, V> createEntry(final OMVRBTreeEntry<K, V> parent) {
    return new OMVRBTreeEntryMemory<K, V>((OMVRBTreeEntryMemory<K, V>) parent, parent.getPageSplitItems());
  }
}
