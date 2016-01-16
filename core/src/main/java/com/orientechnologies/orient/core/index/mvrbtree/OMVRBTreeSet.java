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

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.collection.ONavigableMap;
import com.orientechnologies.common.collection.ONavigableSet;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;

@SuppressWarnings("unchecked")
public class OMVRBTreeSet<E> extends AbstractSet<E> implements ONavigableSet<E>, Cloneable, java.io.Serializable {
  /**
   * The backing map.
   */
  private transient ONavigableMap<E, Object> m;

  // Dummy value to associate with an Object in the backing Map
  private static final Object                PRESENT = new Object();

  /**
   * Constructs a set backed by the specified navigable map.
   */
  OMVRBTreeSet(ONavigableMap<E, Object> m) {
    this.m = m;
  }

  /**
   * Constructs a new, empty tree set, sorted according to the natural ordering of its elements. All elements inserted into the set
   * must implement the {@link Comparable} interface. Furthermore, all such elements must be <i>mutually comparable</i>:
   * {@code e1.compareTo(e2)} must not throw a {@code ClassCastException} for any elements {@code e1} and {@code e2} in the set. If
   * the user attempts to add an element to the set that violates this constraint (for example, the user attempts to add a string
   * element to a set whose elements are integers), the {@code add} call will throw a {@code ClassCastException}.
   */
  public OMVRBTreeSet() {
    this(new OMVRBTreeMemory<E, Object>());
  }

  /**
   * Constructs a new, empty tree set, sorted according to the specified comparator. All elements inserted into the set must be
   * <i>mutually comparable</i> by the specified comparator: {@code comparator.compare(e1, e2)} must not throw a
   * {@code ClassCastException} for any elements {@code e1} and {@code e2} in the set. If the user attempts to add an element to the
   * set that violates this constraint, the {@code add} call will throw a {@code ClassCastException}.
   * 
   * @param comparator
   *          the comparator that will be used to order this set. If {@code null}, the {@linkplain Comparable natural ordering} of
   *          the elements will be used.
   */
  public OMVRBTreeSet(Comparator<? super E> comparator) {
    this(new OMVRBTreeMemory<E, Object>(comparator));
  }

  /**
   * Constructs a new tree set containing the elements in the specified collection, sorted according to the <i>natural ordering</i>
   * of its elements. All elements inserted into the set must implement the {@link Comparable} interface. Furthermore, all such
   * elements must be <i>mutually comparable</i>: {@code e1.compareTo(e2)} must not throw a {@code ClassCastException} for any
   * elements {@code e1} and {@code e2} in the set.
   * 
   * @param c
   *          collection whose elements will comprise the new set
   * @throws ClassCastException
   *           if the elements in {@code c} are not {@link Comparable}, or are not mutually comparable
   * @throws NullPointerException
   *           if the specified collection is null
   */
  public OMVRBTreeSet(Collection<? extends E> c) {
    this();
    addAll(c);
  }

  /**
   * Constructs a new tree set containing the same elements and using the same ordering as the specified sorted set.
   * 
   * @param s
   *          sorted set whose elements will comprise the new set
   * @throws NullPointerException
   *           if the specified sorted set is null
   */
  public OMVRBTreeSet(SortedSet<E> s) {
    this(s.comparator());
    addAll(s);
  }

  /**
   * Returns an iterator over the elements in this set in ascending order.
   * 
   * @return an iterator over the elements in this set in ascending order
   */
  @Override
  public OLazyIterator<E> iterator() {
    return m.navigableKeySet().iterator();
  }

  /**
   * Returns an iterator over the elements in this set in descending order.
   * 
   * @return an iterator over the elements in this set in descending order
   * @since 1.6
   */
  public OLazyIterator<E> descendingIterator() {
    return m.descendingKeySet().iterator();
  }

  /**
   * @since 1.6
   */
  public ONavigableSet<E> descendingSet() {
    return new OMVRBTreeSet<E>(m.descendingMap());
  }

  /**
   * Returns the number of elements in this set (its cardinality).
   * 
   * @return the number of elements in this set (its cardinality)
   */
  @Override
  public int size() {
    return m.size();
  }

  /**
   * Returns {@code true} if this set contains no elements.
   * 
   * @return {@code true} if this set contains no elements
   */
  @Override
  public boolean isEmpty() {
    return m.isEmpty();
  }

  /**
   * Returns {@code true} if this set contains the specified element. More formally, returns {@code true} if and only if this set
   * contains an element {@code e} such that <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
   * 
   * @param o
   *          object to be checked for containment in this set
   * @return {@code true} if this set contains the specified element
   * @throws ClassCastException
   *           if the specified object cannot be compared with the elements currently in the set
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   */
  @Override
  public boolean contains(Object o) {
    return m.containsKey(o);
  }

  /**
   * Adds the specified element to this set if it is not already present. More formally, adds the specified element {@code e} to
   * this set if the set contains no element {@code e2} such that <tt>(e==null&nbsp;?&nbsp;e2==null&nbsp;:&nbsp;e.equals(e2))</tt>.
   * If this set already contains the element, the call leaves the set unchanged and returns {@code false}.
   * 
   * @param e
   *          element to be added to this set
   * @return {@code true} if this set did not already contain the specified element
   * @throws ClassCastException
   *           if the specified object cannot be compared with the elements currently in this set
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   */
  @Override
  public boolean add(E e) {
    return m.put(e, PRESENT) == null;
  }

  /**
   * Removes the specified element from this set if it is present. More formally, removes an element {@code e} such that
   * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>, if this set contains such an element. Returns {@code true} if
   * this set contained the element (or equivalently, if this set changed as a result of the call). (This set will not contain the
   * element once the call returns.)
   * 
   * @param o
   *          object to be removed from this set, if present
   * @return {@code true} if this set contained the specified element
   * @throws ClassCastException
   *           if the specified object cannot be compared with the elements currently in this set
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   */
  @Override
  public boolean remove(Object o) {
    return m.remove(o) == PRESENT;
  }

  /**
   * Removes all of the elements from this set. The set will be empty after this call returns.
   */
  @Override
  public void clear() {
    m.clear();
  }

  /**
   * Adds all of the elements in the specified collection to this set.
   * 
   * @param c
   *          collection containing elements to be added to this set
   * @return {@code true} if this set changed as a result of the call
   * @throws ClassCastException
   *           if the elements provided cannot be compared with the elements currently in the set
   * @throws NullPointerException
   *           if the specified collection is null or if any element is null and this set uses natural ordering, or its comparator
   *           does not permit null elements
   */
  @Override
  public boolean addAll(Collection<? extends E> c) {
    // Use linear-time version if applicable
    if (m.size() == 0 && c.size() > 0 && c instanceof SortedSet && m instanceof OMVRBTree) {
      SortedSet<? extends E> set = (SortedSet<? extends E>) c;
      OMVRBTree<E, Object> map = (OMVRBTree<E, Object>) m;
      Comparator<? super E> cc = (Comparator<? super E>) set.comparator();
      Comparator<? super E> mc = map.comparator();
      if (cc == mc || (cc != null && cc.equals(mc))) {
        map.addAllForOTreeSet(set, PRESENT);
        return true;
      }
    }
    return super.addAll(c);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code fromElement} or {@code toElement} is null and this set uses natural ordering, or its comparator does not
   *           permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public ONavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return new OMVRBTreeSet<E>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code toElement} is null and this set uses natural ordering, or its comparator does not permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public ONavigableSet<E> headSet(E toElement, boolean inclusive) {
    return new OMVRBTreeSet<E>(m.headMap(toElement, inclusive));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code fromElement} is null and this set uses natural ordering, or its comparator does not permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   * @since 1.6
   */
  public ONavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return new OMVRBTreeSet<E>(m.tailMap(fromElement, inclusive));
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code fromElement} or {@code toElement} is null and this set uses natural ordering, or its comparator does not
   *           permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return subSet(fromElement, true, toElement, false);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code toElement} is null and this set uses natural ordering, or its comparator does not permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedSet<E> headSet(E toElement) {
    return headSet(toElement, false);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if {@code fromElement} is null and this set uses natural ordering, or its comparator does not permit null elements
   * @throws IllegalArgumentException
   *           {@inheritDoc}
   */
  public SortedSet<E> tailSet(E fromElement) {
    return tailSet(fromElement, true);
  }

  public Comparator<? super E> comparator() {
    return m.comparator();
  }

  /**
   * @throws NoSuchElementException
   *           {@inheritDoc}
   */
  public E first() {
    return m.firstKey();
  }

  /**
   * @throws NoSuchElementException
   *           {@inheritDoc}
   */
  public E last() {
    return m.lastKey();
  }

  // ONavigableSet API methods

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   * @since 1.6
   */
  public E lower(E e) {
    return m.lowerKey(e);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   * @since 1.6
   */
  public E floor(E e) {
    return m.floorKey(e);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   * @since 1.6
   */
  public E ceiling(E e) {
    return m.ceilingKey(e);
  }

  /**
   * @throws ClassCastException
   *           {@inheritDoc}
   * @throws NullPointerException
   *           if the specified element is null and this set uses natural ordering, or its comparator does not permit null elements
   * @since 1.6
   */
  public E higher(E e) {
    return m.higherKey(e);
  }

  /**
   * @since 1.6
   */
  public E pollFirst() {
    Map.Entry<E, ?> e = m.pollFirstEntry();
    return (e == null) ? null : e.getKey();
  }

  /**
   * @since 1.6
   */
  public E pollLast() {
    Map.Entry<E, ?> e = m.pollLastEntry();
    return (e == null) ? null : e.getKey();
  }

  /**
   * Returns a shallow copy of this {@code OTreeSet} instance. (The elements themselves are not cloned.)
   * 
   * @return a shallow copy of this set
   */
  @Override
  public Object clone() {
    OMVRBTreeSet<E> clone = null;
    try {
      clone = (OMVRBTreeSet<E>) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new OException("Error during mvrbtree clone", e);
    }

    clone.m = new OMVRBTreeMemory<E, Object>(m);
    return clone;
  }

  /**
   * Save the state of the {@code OTreeSet} instance to a stream (that is, serialize it).
   * 
   * @serialData Emits the comparator used to order this set, or {@code null} if it obeys its elements' natural ordering (Object),
   *             followed by the size of the set (the number of elements it contains) (int), followed by all of its elements (each
   *             an Object) in order (as determined by the set's Comparator, or by the elements' natural ordering if the set has no
   *             Comparator).
   */
  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    // Write out any hidden stuff
    s.defaultWriteObject();

    // Write out Comparator
    s.writeObject(m.comparator());

    // Write out size
    s.writeInt(m.size());

    // Write out all elements in the proper order.
    for (Iterator<E> i = m.keySet().iterator(); i.hasNext();)
      s.writeObject(i.next());
  }

  /**
   * Reconstitute the {@code OTreeSet} instance from a stream (that is, deserialize it).
   */
  private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
    // Read in any hidden stuff
    s.defaultReadObject();

    // Read in Comparator
    Comparator<? super E> c = (Comparator<? super E>) s.readObject();

    // Create backing OMVRBTree
    OMVRBTree<E, Object> tm;
    if (c == null)
      tm = new OMVRBTreeMemory<E, Object>();
    else
      tm = new OMVRBTreeMemory<E, Object>(c);
    m = tm;

    // Read in size
    int size = s.readInt();

    tm.readOTreeSet(size, s, PRESENT);
  }

  private static final long serialVersionUID = -2479143000061671589L;
}
