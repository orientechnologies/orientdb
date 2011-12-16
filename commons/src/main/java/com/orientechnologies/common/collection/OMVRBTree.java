/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;

/**
 * Base abstract class of MVRB-Tree algorithm.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <K>
 *          Key type
 * @param <V>
 *          Value type
 */
@SuppressWarnings({ "unchecked", "serial" })
public abstract class OMVRBTree<K, V> extends AbstractMap<K, V> implements ONavigableMap<K, V>, Cloneable, java.io.Serializable {
	boolean																		pageItemFound				= false;
	protected int															pageItemComparator	= 0;
	protected int															pageIndex						= -1;

	protected float														pageLoadFactor			= 0.7f;

	/**
	 * The comparator used to maintain order in this tree map, or null if it uses the natural ordering of its keys.
	 * 
	 * @serial
	 */
	protected final Comparator<? super K>			comparator;
	protected transient OMVRBTreeEntry<K, V>	root								= null;

	/**
	 * The number of structural modifications to the tree.
	 */
	transient int															modCount						= 0;
	protected transient boolean								runtimeCheckEnabled	= false;
	protected transient boolean								debug								= false;

	protected Object													lastSearchKey;
	protected OMVRBTreeEntry<K, V>						lastSearchNode;
	protected boolean													lastSearchFound			= false;
	protected int															lastSearchIndex			= -1;

/**
	 * Indicates search behaviour in case of {@link OCompositeKey) keys that have less amount of internal keys are used, whether
	 * lowest or highest partially matched key should be used. Such keys is allowed to use only in
	 * 
	 * @link OMVRBTree#subMap(K, boolean, K, boolean)}, {@link OMVRBTree#tailMap(K, boolean, K, boolean)} and
	 *       {@link OMVRBTree#headMap(K, boolean, K, boolean)} .
	 */
	public static enum PartialSearchMode {
		/**
		 * Any partially matched key will be used as search result.
		 */
		NONE,
		/**
		 * The biggest partially matched key will be used as search result.
		 */
		HIGHEST_BOUNDARY,

		/**
		 * The smallest partially matched key will be used as search result.
		 */
		LOWEST_BOUNDARY
	}

	/**
	 * Constructs a new, empty tree map, using the natural ordering of its keys. All keys inserted into the map must implement the
	 * {@link Comparable} interface. Furthermore, all such keys must be <i>mutually comparable</i>: <tt>k1.compareTo(k2)</tt> must not
	 * throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt> in the map. If the user attempts to put a key into
	 * the map that violates this constraint (for example, the user attempts to put a string key into a map whose keys are integers),
	 * the <tt>put(Object key, Object value)</tt> call will throw a <tt>ClassCastException</tt>.
	 */
	public OMVRBTree() {
		init();
		comparator = null;
	}

	/**
	 * Constructs a new, empty tree map, ordered according to the given comparator. All keys inserted into the map must be <i>mutually
	 * comparable</i> by the given comparator: <tt>comparator.compare(k1,
	 * k2)</tt> must not throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt> in the map. If the user attempts
	 * to put a key into the map that violates this constraint, the <tt>put(Object
	 * key, Object value)</tt> call will throw a <tt>ClassCastException</tt>.
	 * 
	 * @param iComparator
	 *          the comparator that will be used to order this map. If <tt>null</tt>, the {@linkplain Comparable natural ordering} of
	 *          the keys will be used.
	 */
	public OMVRBTree(final Comparator<? super K> iComparator) {
		init();
		this.comparator = iComparator;
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
	public OMVRBTree(final Map<? extends K, ? extends V> m) {
		init();
		comparator = null;
		putAll(m);
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
	public OMVRBTree(final SortedMap<K, ? extends V> m) {
		init();
		comparator = m.comparator();
		try {
			buildFromSorted(m.size(), m.entrySet().iterator(), null, null);
		} catch (java.io.IOException cannotHappen) {
		} catch (ClassNotFoundException cannotHappen) {
		}
	}

	/**
	 * Create a new entry with the first key/value to handle.
	 */
	protected abstract OMVRBTreeEntry<K, V> createEntry(final K key, final V value);

	/**
	 * Create a new node with the same parent of the node is splitting.
	 */
	protected abstract OMVRBTreeEntry<K, V> createEntry(final OMVRBTreeEntry<K, V> parent);

	public int getNodes() {
		int counter = -1;

		OMVRBTreeEntry<K, V> entry = getFirstEntry();
		while (entry != null) {
			entry = successor(entry);
			counter++;
		}

		return counter;
	}

	protected abstract void setSize(int iSize);

	public abstract int getDefaultPageSize();

	/**
	 * Returns <tt>true</tt> if this map contains a mapping for the specified key.
	 * 
	 * @param key
	 *          key whose presence in this map is to be tested
	 * @return <tt>true</tt> if this map contains a mapping for the specified key
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	@Override
	public boolean containsKey(final Object key) {
		return getEntry(key, PartialSearchMode.NONE) != null;
	}

	/**
	 * Returns <tt>true</tt> if this map maps one or more keys to the specified value. More formally, returns <tt>true</tt> if and
	 * only if this map contains at least one mapping to a value <tt>v</tt> such that
	 * <tt>(value==null ? v==null : value.equals(v))</tt>. This operation will probably require time linear in the map size for most
	 * implementations.
	 * 
	 * @param value
	 *          value whose presence in this map is to be tested
	 * @return <tt>true</tt> if a mapping to <tt>value</tt> exists; <tt>false</tt> otherwise
	 * @since 1.2
	 */
	@Override
	public boolean containsValue(final Object value) {
		for (OMVRBTreeEntry<K, V> e = getFirstEntry(); e != null; e = next(e))
			if (valEquals(value, e.getValue()))
				return true;
		return false;
	}

	/**
	 * Returns the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the key.
	 * 
	 * <p>
	 * More formally, if this map contains a mapping from a key {@code k} to a value {@code v} such that {@code key} compares equal to
	 * {@code k} according to the map's ordering, then this method returns {@code v}; otherwise it returns {@code null}. (There can be
	 * at most one such mapping.)
	 * 
	 * <p>
	 * A return value of {@code null} does not <i>necessarily</i> indicate that the map contains no mapping for the key; it's also
	 * possible that the map explicitly maps the key to {@code null}. The {@link #containsKey containsKey} operation may be used to
	 * distinguish these two cases.
	 * 
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	@Override
	public V get(final Object key) {
		if (size() == 0)
			return null;

		OMVRBTreeEntry<K, V> entry = null;

		final long timer = OProfiler.getInstance().startChrono();

		try {
			// TRY TO GET LATEST SEARCH
			final OMVRBTreeEntry<K, V> node = getLastSearchNodeForSameKey(key);
			if (node != null) {
				// SAME SEARCH OF PREVIOUS ONE: REUSE LAST RESULT?
				if (lastSearchFound)
					// REUSE LAST RESULT, OTHERWISE THE KEY NOT EXISTS
					return node.getValue(lastSearchIndex);
			} else
				// SEARCH THE ITEM
				entry = getEntry(key, PartialSearchMode.NONE);

			return entry == null ? null : entry.getValue();

		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTree.get", timer);
		}
	}

	public Comparator<? super K> comparator() {
		return comparator;
	}

	/**
	 * @throws NoSuchElementException
	 *           {@inheritDoc}
	 */
	public K firstKey() {
		return key(getFirstEntry());
	}

	/**
	 * @throws NoSuchElementException
	 *           {@inheritDoc}
	 */
	public K lastKey() {
		return key(getLastEntry());
	}

	/**
	 * Copies all of the mappings from the specified map to this map. These mappings replace any mappings that this map had for any of
	 * the keys currently in the specified map.
	 * 
	 * @param map
	 *          mappings to be stored in this map
	 * @throws ClassCastException
	 *           if the class of a key or value in the specified map prevents it from being stored in this map
	 * @throws NullPointerException
	 *           if the specified map is null or the specified map contains a null key and this map does not permit null keys
	 */
	@Override
	public void putAll(final Map<? extends K, ? extends V> map) {
		int mapSize = map.size();
		if (size() == 0 && mapSize != 0 && map instanceof SortedMap) {
			Comparator<?> c = ((SortedMap<? extends K, ? extends V>) map).comparator();
			if (c == comparator || (c != null && c.equals(comparator))) {
				++modCount;
				try {
					buildFromSorted(mapSize, map.entrySet().iterator(), null, null);
				} catch (java.io.IOException cannotHappen) {
				} catch (ClassNotFoundException cannotHappen) {
				}
				return;
			}
		}
		super.putAll(map);
	}

	/**
	 * Returns this map's entry for the given key, or <tt>null</tt> if the map does not contain an entry for the key.
	 * 
	 * In case of {@link OCompositeKey} keys you can specify which key can be used: lowest, highest, any.
	 * 
	 * @param key
	 *          Key to search.
	 * @param partialSearchMode
	 *          Which key can be used in case of {@link OCompositeKey} key is passed in.
	 * 
	 * @return this map's entry for the given key, or <tt>null</tt> if the map does not contain an entry for the key
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	public final OMVRBTreeEntry<K, V> getEntry(final Object key, final PartialSearchMode partialSearchMode) {
		return getEntry(key, false, partialSearchMode);
	}

	final OMVRBTreeEntry<K, V> getEntry(final Object key, final boolean iGetContainer, final PartialSearchMode partialSearchMode) {
		if (key == null)
			return setLastSearchNode(null, null);

		pageItemFound = false;

		if (size() == 0) {
			pageIndex = 0;
			return iGetContainer ? root : null;
		}

		OMVRBTreeEntry<K, V> p = getBestEntryPoint((K) key);

		checkTreeStructure(p);

		if (p == null)
			return setLastSearchNode(key, null);

		OMVRBTreeEntry<K, V> lastNode = p;
		OMVRBTreeEntry<K, V> prevNode = null;
		OMVRBTreeEntry<K, V> tmpNode;
		int beginKey = -1;
		int steps = -1;
		final Comparable<? super K> k = (Comparable<? super K>) key;

		try {
			while (p != null && p.getSize() > 0) {
				searchNodeCallback();
				steps++;

				lastNode = p;

				if (comparator != null)
					beginKey = comparator.compare((K) key, p.getFirstKey());
				else
					try {
						beginKey = k.compareTo(p.getFirstKey());
					} catch (Exception e) {
					}

				if (beginKey == 0) {
					// EXACT MATCH, YOU'RE VERY LUCKY: RETURN THE FIRST KEY WITHOUT SEARCH INSIDE THE NODE
					pageIndex = 0;
					pageItemFound = true;
					pageItemComparator = 0;

					if (((Comparable<?>) k) instanceof OCompositeKey)
						return adjustSearchResult(partialSearchMode, p, k);

					return setLastSearchNode(key, p);
				}

				if (comparator != null)
					pageItemComparator = comparator.compare((K) key, p.getLastKey());
				else
					pageItemComparator = k.compareTo(p.getLastKey());

				if (beginKey < 0) {
					if (pageItemComparator < 0) {
						tmpNode = predecessor(p);
						if (tmpNode != null && tmpNode != prevNode) {
							// MINOR THAN THE CURRENT: GET THE LEFT NODE
							prevNode = p;
							p = tmpNode;
							continue;
						}
					}
				} else if (beginKey > 0) {
					if (pageItemComparator > 0) {
						tmpNode = successor(p);
						if (tmpNode != null && tmpNode != prevNode) {
							// MAJOR THAN THE CURRENT: GET THE RIGHT NODE
							prevNode = p;
							p = tmpNode;
							continue;
						}
					}
				}

				// SEARCH INSIDE THE NODE
				final V value = lastNode.search(k);

				setLastSearchNode(key, lastNode);

				// PROBABLY PARTIAL KEY IS FOUND USE SEARCH MODE TO FIND PREFERRED ONE
				if (value != null && key instanceof OCompositeKey)
					return adjustSearchResult(partialSearchMode, p, k);

				if (value != null || iGetContainer)
					// FOUND: RETURN CURRENT NODE OR AT LEAST THE CONTAINER NODE
					return lastNode;

				// NOT FOUND
				return null;
			}
		} finally {
			checkTreeStructure(p);

			OProfiler.getInstance().updateStat("[OMVRBTree.getEntry] Steps of search", steps);
		}

		return setLastSearchNode(key, null);
	}

	private OMVRBTreeEntry<K, V> adjustSearchResult(PartialSearchMode partialSearchMode, OMVRBTreeEntry<K, V> p,
			Comparable<? extends Object> k) {
		final OCompositeKey foundKey = (OCompositeKey) p.getKey();
		final OCompositeKey keyToFind = (OCompositeKey) k;

		if (keyToFind.getKeys().size() >= foundKey.getKeys().size())
			return p;

		switch (partialSearchMode) {
		case NONE:
			return p;
		case LOWEST_BOUNDARY:
			return findLowestBoundary(p, k);
		case HIGHEST_BOUNDARY:
			return findHighestBoundary(p, k);
		default:
			throw new OException("Invalid value of search mode (" + partialSearchMode + ").");
		}
	}

	/**
	 * Basic implementation that returns the root node.
	 */
	protected OMVRBTreeEntry<K, V> getBestEntryPoint(final K key) {
		return root;
	}

	/**
	 * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry for the least key greater than
	 * the specified key; if no such entry exists (i.e., the greatest key in the Tree is less than the specified key), returns
	 * <tt>null</tt>.
	 * 
	 * @param key
	 *          Key to search.
	 * @param partialSearchMode
	 *          In case of {@link OCompositeKey} key is passed in this parameter will be used to find preferred one.
	 */
	public final OMVRBTreeEntry<K, V> getCeilingEntry(final K key, final PartialSearchMode partialSearchMode) {
		final OMVRBTreeEntry<K, V> p = getEntry(key, true, partialSearchMode);

		if (p == null)
			return null;

		if (pageItemFound)
			return p;
		// NOT MATCHED, POSITION IS ALREADY TO THE NEXT ONE
		else if (pageIndex < p.getSize())
			if (key instanceof OCompositeKey) {
				final OCompositeKey keyToSearch = (OCompositeKey) key;
				final OCompositeKey foundKey = (OCompositeKey) p.getKey();

				if (keyToSearch.getKeys().size() < foundKey.getKeys().size()) {
					final OCompositeKey keyToAdjust = new OCompositeKey(foundKey.getKeys().subList(0, keyToSearch.getKeys().size()));
					return adjustSearchResult(partialSearchMode, p, keyToAdjust);
				}
			} else {
				return p;
			}
		return null;

	}

	/**
	 * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry for the greatest key less than
	 * the specified key; if no such entry exists, returns <tt>null</tt>.
	 * 
	 * @param key
	 *          Key to search.
	 * @param partialSearchMode
	 *          In case of {@link OCompositeKey} composite key is passed in this parameter will be used to find preferred one.
	 */
	public final OMVRBTreeEntry<K, V> getFloorEntry(final K key, final PartialSearchMode partialSearchMode) {
		final OMVRBTreeEntry<K, V> p = getEntry(key, true, partialSearchMode);

		if (p == null)
			return null;

		if (pageItemFound)
			return p;

		final OMVRBTreeEntry<K, V> adjacentEntry = previous(p);
		if (key instanceof OCompositeKey) {
			final OCompositeKey keyToSearch = (OCompositeKey) key;
			final OCompositeKey foundKey = (OCompositeKey) adjacentEntry.getKey();

			if (keyToSearch.getKeys().size() < foundKey.getKeys().size()) {
				final OCompositeKey keyToAdjust = new OCompositeKey(foundKey.getKeys().subList(0, keyToSearch.getKeys().size()));
				return adjustSearchResult(partialSearchMode, adjacentEntry, keyToAdjust);
			}
		}
		return adjacentEntry;
	}

	/**
	 * Gets the entry for the least key greater than the specified key; if no such entry exists, returns the entry for the least key
	 * greater than the specified key; if no such entry exists returns <tt>null</tt>.
	 */
	public final OMVRBTreeEntry<K, V> getHigherEntry(final K key) {
		final OMVRBTreeEntry<K, V> p = getEntry(key, true, PartialSearchMode.HIGHEST_BOUNDARY);

		if (p == null)
			return null;

		if (pageItemFound)
			// MATCH, RETURN THE NEXT ONE
			return next(p);
		else if (pageIndex < p.getSize())
			// NOT MATCHED, POSITION IS ALREADY TO THE NEXT ONE
			return p;

		return null;
	}

	/**
	 * Returns the entry for the greatest key less than the specified key; if no such entry exists (i.e., the least key in the Tree is
	 * greater than the specified key), returns <tt>null</tt>.
	 */
	public final OMVRBTreeEntry<K, V> getLowerEntry(final K key) {
		final OMVRBTreeEntry<K, V> p = getEntry(key, true, PartialSearchMode.LOWEST_BOUNDARY);

		if (p == null)
			return null;

		return previous(p);
	}

	private OMVRBTreeEntry<K, V> findHighestBoundary(final OMVRBTreeEntry<K, V> p, final Comparable<? extends Object> key) {
		OMVRBTreeEntry<K, V> currentEntry = p;
		int currentIndex = pageIndex;

		int result;
		do {
			OMVRBTreeEntry<K, V> nextEntry = next(currentEntry);

			if (nextEntry == null) {
				break;
			}

			final Comparable<?> nextKey = (Comparable<?>) nextEntry.getKey();

			result = ((Comparable<Object>) key).compareTo(nextKey);

			if (result == 0) {
				currentEntry = nextEntry;
				currentIndex = pageIndex;
			}

		} while (result == 0);

		pageIndex = currentIndex;
		return currentEntry;
	}

	private OMVRBTreeEntry<K, V> findLowestBoundary(final OMVRBTreeEntry<K, V> p, final Comparable<? extends Object> key) {
		OMVRBTreeEntry<K, V> currentEntry = p;
		int currentIndex = pageIndex;

		int result;
		do {
			OMVRBTreeEntry<K, V> prevEntry = previous(currentEntry);

			if (prevEntry == null) {
				break;
			}

			final Comparable<?> prevKey = (Comparable<?>) prevEntry.getKey();

			result = ((Comparable<Object>) key).compareTo(prevKey);

			if (result == 0) {
				currentEntry = prevEntry;
				currentIndex = pageIndex;
			}

		} while (result == 0);

		pageIndex = currentIndex;
		return currentEntry;
	}

	/**
	 * Associates the specified value with the specified key in this map. If the map previously contained a mapping for the key, the
	 * old value is replaced.
	 * 
	 * @param key
	 *          key with which the specified value is to be associated
	 * @param value
	 *          value to be associated with the specified key
	 * 
	 * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for <tt>key</tt>. (A
	 *         <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt> with <tt>key</tt>.)
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	@Override
	public V put(final K key, final V value) {
		OMVRBTreeEntry<K, V> parentNode = null;

		try {
			if (root == null) {
				root = createEntry(key, value);
				root.setColor(BLACK);

				setSize(1);
				modCount++;
				return null;
			}

			// TRY TO GET LATEST SEARCH
			parentNode = getLastSearchNodeForSameKey(key);
			if (parentNode != null) {
				if (lastSearchFound) {
					// EXACT MATCH: UPDATE THE VALUE
					pageIndex = lastSearchIndex;
					modCount++;
					return parentNode.setValue(value);
				}
			}

			// SEARCH THE ITEM
			parentNode = getEntry(key, true, PartialSearchMode.NONE);

			if (pageItemFound) {
				modCount++;
				// EXACT MATCH: UPDATE THE VALUE
				return parentNode.setValue(value);
			}

			setLastSearchNode(null, null);

			if (parentNode == null) {
				parentNode = root;
				pageIndex = 0;
			}

			if (parentNode.getFreeSpace() > 0) {
				// INSERT INTO THE PAGE
				parentNode.insert(pageIndex, key, value);
			} else {
				// CREATE NEW NODE AND COPY HALF OF VALUES FROM THE ORIGIN TO THE NEW ONE IN ORDER TO GET VALUES BALANCED
				final OMVRBTreeEntry<K, V> newNode = createEntry(parentNode);

				if (pageIndex < parentNode.getPageSplitItems())
					// INSERT IN THE ORIGINAL NODE
					parentNode.insert(pageIndex, key, value);
				else
					// INSERT IN THE NEW NODE
					newNode.insert(pageIndex - parentNode.getPageSplitItems(), key, value);

				OMVRBTreeEntry<K, V> node = parentNode.getRight();
				OMVRBTreeEntry<K, V> prevNode = parentNode;
				int cmp = 0;
				if (comparator != null)
					while (node != null) {
						cmp = comparator.compare(newNode.getFirstKey(), node.getFirstKey());
						if (cmp < 0) {
							prevNode = node;
							node = node.getLeft();
						} else if (cmp > 0) {
							prevNode = node;
							node = node.getRight();
						} else {
							throw new IllegalStateException("Duplicated keys were found in OMVRBTree.");
						}
					}
				else
					while (node != null) {
						cmp = ((Comparable<K>) newNode.getFirstKey()).compareTo(node.getFirstKey());
						if (cmp < 0) {
							prevNode = node;
							node = node.getLeft();
						} else if (cmp > 0) {
							prevNode = node;
							node = node.getRight();
						} else {
							throw new IllegalStateException("Duplicated keys were found in OMVRBTree.");
						}
					}

				if (prevNode == parentNode)
					parentNode.setRight(newNode);
				else if (cmp < 0)
					prevNode.setLeft(newNode);
				else if (cmp > 0)
					prevNode.setRight(newNode);
				else
					throw new IllegalStateException("Duplicated keys were found in OMVRBTree.");

				fixAfterInsertion(newNode);
			}

			modCount++;
			setSizeDelta(+1);

		} finally {
			checkTreeStructure(parentNode);
		}

		return null;
	}

	/**
	 * Removes the mapping for this key from this OMVRBTree if present.
	 * 
	 * @param key
	 *          key for which mapping should be removed
	 * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for <tt>key</tt>. (A
	 *         <tt>null</tt> return can also indicate that the map previously associated <tt>null</tt> with <tt>key</tt>.)
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	@Override
	public V remove(final Object key) {
		OMVRBTreeEntry<K, V> p = getEntry(key, PartialSearchMode.NONE);
		setLastSearchNode(null, null);
		if (p == null)
			return null;

		V oldValue = p.getValue();
		deleteEntry(p);
		return oldValue;
	}

	/**
	 * Removes all of the mappings from this map. The map will be empty after this call returns.
	 */
	@Override
	public void clear() {
		modCount++;
		setSize(0);
		setLastSearchNode(null, null);
		setRoot(null);
	}

	/**
	 * Returns a shallow copy of this <tt>OMVRBTree</tt> instance. (The keys and values themselves are not cloned.)
	 * 
	 * @return a shallow copy of this map
	 */
	@Override
	public Object clone() {
		OMVRBTree<K, V> clone = null;
		try {
			clone = (OMVRBTree<K, V>) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError();
		}

		// Put clone into "virgin" state (except for comparator)
		clone.pageIndex = pageIndex;
		clone.pageItemFound = pageItemFound;
		clone.pageLoadFactor = pageLoadFactor;

		clone.root = null;
		clone.setSize(0);
		clone.modCount = 0;
		clone.entrySet = null;
		clone.navigableKeySet = null;
		clone.descendingMap = null;

		// Initialize clone with our mappings
		try {
			clone.buildFromSorted(size(), entrySet().iterator(), null, null);
		} catch (java.io.IOException cannotHappen) {
		} catch (ClassNotFoundException cannotHappen) {
		}

		return clone;
	}

	// ONavigableMap API methods

	/**
	 * @since 1.6
	 */
	public Map.Entry<K, V> firstEntry() {
		return exportEntry(getFirstEntry());
	}

	/**
	 * @since 1.6
	 */
	public Map.Entry<K, V> lastEntry() {
		return exportEntry(getLastEntry());
	}

	/**
	 * @since 1.6
	 */
	public Entry<K, V> pollFirstEntry() {
		OMVRBTreeEntry<K, V> p = getFirstEntry();
		Map.Entry<K, V> result = exportEntry(p);
		if (p != null)
			deleteEntry(p);
		return result;
	}

	/**
	 * @since 1.6
	 */
	public Entry<K, V> pollLastEntry() {
		OMVRBTreeEntry<K, V> p = getLastEntry();
		Map.Entry<K, V> result = exportEntry(p);
		if (p != null)
			deleteEntry(p);
		return result;
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> lowerEntry(final K key) {
		return exportEntry(getLowerEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K lowerKey(final K key) {
		return keyOrNull(getLowerEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> floorEntry(final K key) {
		return exportEntry(getFloorEntry(key, PartialSearchMode.NONE));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K floorKey(final K key) {
		return keyOrNull(getFloorEntry(key, PartialSearchMode.NONE));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> ceilingEntry(final K key) {
		return exportEntry(getCeilingEntry(key, PartialSearchMode.NONE));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K ceilingKey(final K key) {
		return keyOrNull(getCeilingEntry(key, PartialSearchMode.NONE));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> higherEntry(final K key) {
		return exportEntry(getHigherEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K higherKey(final K key) {
		return keyOrNull(getHigherEntry(key));
	}

	// Views

	/**
	 * Fields initialized to contain an instance of the entry set view the first time this view is requested. Views are stateless, so
	 * there's no reason to create more than one.
	 */
	private transient EntrySet						entrySet				= null;
	private transient KeySet<K>						navigableKeySet	= null;
	private transient ONavigableMap<K, V>	descendingMap		= null;

	/**
	 * Returns a {@link Set} view of the keys contained in this map. The set's iterator returns the keys in ascending order. The set
	 * is backed by the map, so changes to the map are reflected in the set, and vice-versa. If the map is modified while an iteration
	 * over the set is in progress (except through the iterator's own <tt>remove</tt> operation), the results of the iteration are
	 * undefined. The set supports element removal, which removes the corresponding mapping from the map, via the
	 * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt> operations. It does
	 * not support the <tt>add</tt> or <tt>addAll</tt> operations.
	 */
	@Override
	public Set<K> keySet() {
		return navigableKeySet();
	}

	/**
	 * @since 1.6
	 */
	public ONavigableSet<K> navigableKeySet() {
		final KeySet<K> nks = navigableKeySet;
		return (nks != null) ? nks : (navigableKeySet = (KeySet<K>) new KeySet<Object>((ONavigableMap<Object, Object>) this));
	}

	/**
	 * @since 1.6
	 */
	public ONavigableSet<K> descendingKeySet() {
		return descendingMap().navigableKeySet();
	}

	/**
	 * Returns a {@link Collection} view of the values contained in this map. The collection's iterator returns the values in
	 * ascending order of the corresponding keys. The collection is backed by the map, so changes to the map are reflected in the
	 * collection, and vice-versa. If the map is modified while an iteration over the collection is in progress (except through the
	 * iterator's own <tt>remove</tt> operation), the results of the iteration are undefined. The collection supports element removal,
	 * which removes the corresponding mapping from the map, via the <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>,
	 * <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt> operations. It does not support the <tt>add</tt> or <tt>addAll</tt>
	 * operations.
	 */
	@Override
	public Collection<V> values() {
		final Collection<V> vs = super.values();
		return (vs != null) ? vs : null;
	}

	/**
	 * Returns a {@link Set} view of the mappings contained in this map. The set's iterator returns the entries in ascending key
	 * order. The set is backed by the map, so changes to the map are reflected in the set, and vice-versa. If the map is modified
	 * while an iteration over the set is in progress (except through the iterator's own <tt>remove</tt> operation, or through the
	 * <tt>setValue</tt> operation on a map entry returned by the iterator) the results of the iteration are undefined. The set
	 * supports element removal, which removes the corresponding mapping from the map, via the <tt>Iterator.remove</tt>,
	 * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt> operations. It does not support the <tt>add</tt>
	 * or <tt>addAll</tt> operations.
	 */
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		final EntrySet es = entrySet;
		return (es != null) ? es : (entrySet = new EntrySet());
	}

	/**
	 * @since 1.6
	 */
	public ONavigableMap<K, V> descendingMap() {
		final ONavigableMap<K, V> km = descendingMap;
		return (km != null) ? km : (descendingMap = new DescendingSubMap<K, V>(this, true, null, true, true, null, true));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>fromKey</tt> or <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does not permit
	 *           null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 * @since 1.6
	 */
	public ONavigableMap<K, V> subMap(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive) {
		return new AscendingSubMap<K, V>(this, false, fromKey, fromInclusive, false, toKey, toInclusive);
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 * @since 1.6
	 */
	public ONavigableMap<K, V> headMap(final K toKey, final boolean inclusive) {
		return new AscendingSubMap<K, V>(this, true, null, true, false, toKey, inclusive);
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>fromKey</tt> is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 * @since 1.6
	 */
	public ONavigableMap<K, V> tailMap(final K fromKey, final boolean inclusive) {
		return new AscendingSubMap<K, V>(this, false, fromKey, inclusive, true, null, true);
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>fromKey</tt> or <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does not permit
	 *           null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 */
	public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
		return subMap(fromKey, true, toKey, false);
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>toKey</tt> is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 */
	public SortedMap<K, V> headMap(final K toKey) {
		return headMap(toKey, false);
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if <tt>fromKey</tt> is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @throws IllegalArgumentException
	 *           {@inheritDoc}
	 */
	public SortedMap<K, V> tailMap(final K fromKey) {
		return tailMap(fromKey, true);
	}

	// View class support

	class Values extends AbstractCollection<V> {
		@Override
		public Iterator<V> iterator() {
			return new ValueIterator(getFirstEntry());
		}

		@Override
		public int size() {
			return OMVRBTree.this.size();
		}

		@Override
		public boolean contains(final Object o) {
			return OMVRBTree.this.containsValue(o);
		}

		@Override
		public boolean remove(final Object o) {
			for (OMVRBTreeEntry<K, V> e = getFirstEntry(); e != null; e = next(e)) {
				if (valEquals(e.getValue(), o)) {
					deleteEntry(e);
					return true;
				}
			}
			return false;
		}

		@Override
		public void clear() {
			OMVRBTree.this.clear();
		}
	}

	class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator(getFirstEntry());
		}

		@Override
		public boolean contains(final Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			OMVRBTreeEntry<K, V> entry = (OMVRBTreeEntry<K, V>) o;
			final V value = entry.getValue();
			final V p = get(entry.getKey());
			return p != null && valEquals(p, value);
		}

		@Override
		public boolean remove(final Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			final OMVRBTreeEntry<K, V> entry = (OMVRBTreeEntry<K, V>) o;
			final V value = entry.getValue();
			OMVRBTreeEntry<K, V> p = getEntry(entry.getKey(), PartialSearchMode.NONE);
			if (p != null && valEquals(p.getValue(), value)) {
				deleteEntry(p);
				return true;
			}
			return false;
		}

		@Override
		public int size() {
			return OMVRBTree.this.size();
		}

		@Override
		public void clear() {
			OMVRBTree.this.clear();
		}
	}

	/*
	 * Unlike Values and EntrySet, the KeySet class is static, delegating to a ONavigableMap to allow use by SubMaps, which outweighs
	 * the ugliness of needing type-tests for the following Iterator methods that are defined appropriately in main versus submap
	 * classes.
	 */

	OLazyIterator<K> keyIterator() {
		return new KeyIterator(getFirstEntry());
	}

	OLazyIterator<K> descendingKeyIterator() {
		return new DescendingKeyIterator(getLastEntry());
	}

	@SuppressWarnings("rawtypes")
	static final class KeySet<E> extends AbstractSet<E> implements ONavigableSet<E> {
		private final ONavigableMap<E, Object>	m;

		KeySet(ONavigableMap<E, Object> map) {
			m = map;
		}

		@Override
		public OLazyIterator<E> iterator() {
			if (m instanceof OMVRBTree)
				return ((OMVRBTree<E, Object>) m).keyIterator();
			else
				return (((OMVRBTree.NavigableSubMap) m).keyIterator());
		}

		public OLazyIterator<E> descendingIterator() {
			if (m instanceof OMVRBTree)
				return ((OMVRBTree<E, Object>) m).descendingKeyIterator();
			else
				return (((OMVRBTree.NavigableSubMap) m).descendingKeyIterator());
		}

		@Override
		public int size() {
			return m.size();
		}

		@Override
		public boolean isEmpty() {
			return m.isEmpty();
		}

		@Override
		public boolean contains(final Object o) {
			return m.containsKey(o);
		}

		@Override
		public void clear() {
			m.clear();
		}

		public E lower(final E e) {
			return m.lowerKey(e);
		}

		public E floor(final E e) {
			return m.floorKey(e);
		}

		public E ceiling(final E e) {
			return m.ceilingKey(e);
		}

		public E higher(final E e) {
			return m.higherKey(e);
		}

		public E first() {
			return m.firstKey();
		}

		public E last() {
			return m.lastKey();
		}

		public Comparator<? super E> comparator() {
			return m.comparator();
		}

		public E pollFirst() {
			final Map.Entry<E, Object> e = m.pollFirstEntry();
			return e == null ? null : e.getKey();
		}

		public E pollLast() {
			final Map.Entry<E, Object> e = m.pollLastEntry();
			return e == null ? null : e.getKey();
		}

		@Override
		public boolean remove(final Object o) {
			final int oldSize = size();
			m.remove(o);
			return size() != oldSize;
		}

		public ONavigableSet<E> subSet(final E fromElement, final boolean fromInclusive, final E toElement, final boolean toInclusive) {
			return new OMVRBTreeSet<E>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
		}

		public ONavigableSet<E> headSet(final E toElement, final boolean inclusive) {
			return new OMVRBTreeSet<E>(m.headMap(toElement, inclusive));
		}

		public ONavigableSet<E> tailSet(final E fromElement, final boolean inclusive) {
			return new OMVRBTreeSet<E>(m.tailMap(fromElement, inclusive));
		}

		public SortedSet<E> subSet(final E fromElement, final E toElement) {
			return subSet(fromElement, true, toElement, false);
		}

		public SortedSet<E> headSet(final E toElement) {
			return headSet(toElement, false);
		}

		public SortedSet<E> tailSet(final E fromElement) {
			return tailSet(fromElement, true);
		}

		public ONavigableSet<E> descendingSet() {
			return new OMVRBTreeSet<E>(m.descendingMap());
		}
	}

	final class EntryIterator extends AbstractEntryIterator<K, V, Map.Entry<K, V>> {
		EntryIterator(final OMVRBTreeEntry<K, V> first) {
			super(first);
		}

		public Map.Entry<K, V> next() {
			return nextEntry();
		}
	}

	final class ValueIterator extends AbstractEntryIterator<K, V, V> {
		ValueIterator(final OMVRBTreeEntry<K, V> first) {
			super(first);
		}

		public V next() {
			return nextValue();
		}
	}

	final class KeyIterator extends AbstractEntryIterator<K, V, K> {
		KeyIterator(final OMVRBTreeEntry<K, V> first) {
			super(first);
		}

		public K next() {
			return nextKey();
		}
	}

	final class DescendingKeyIterator extends AbstractEntryIterator<K, V, K> {
		DescendingKeyIterator(final OMVRBTreeEntry<K, V> first) {
			super(first);
		}

		public K next() {
			return prevEntry().getKey();
		}
	}

	// Little utilities

	/**
	 * Compares two keys using the correct comparison method for this OMVRBTree.
	 */
	final int compare(final Object k1, final Object k2) {
		return comparator == null ? ((Comparable<? super K>) k1).compareTo((K) k2) : comparator.compare((K) k1, (K) k2);
	}

	/**
	 * Test two values for equality. Differs from o1.equals(o2) only in that it copes with <tt>null</tt> o1 properly.
	 */
	final static boolean valEquals(final Object o1, final Object o2) {
		return (o1 == null ? o2 == null : o1.equals(o2));
	}

	/**
	 * Return SimpleImmutableEntry for entry, or null if null
	 */
	static <K, V> Map.Entry<K, V> exportEntry(final OMVRBTreeEntry<K, V> omvrbTreeEntryPosition) {
		return omvrbTreeEntryPosition == null ? null : new OSimpleImmutableEntry<K, V>(omvrbTreeEntryPosition);
	}

	/**
	 * Return SimpleImmutableEntry for entry, or null if null
	 */
	static <K, V> Map.Entry<K, V> exportEntry(final OMVRBTreeEntryPosition<K, V> omvrbTreeEntryPosition) {
		return omvrbTreeEntryPosition == null ? null : new OSimpleImmutableEntry<K, V>(omvrbTreeEntryPosition.entry);
	}

	/**
	 * Return key for entry, or null if null
	 */
	static <K, V> K keyOrNull(final OMVRBTreeEntry<K, V> e) {
		return e == null ? null : e.getKey();
	}

	/**
	 * Return key for entry, or null if null
	 */
	static <K, V> K keyOrNull(OMVRBTreeEntryPosition<K, V> e) {
		return e == null ? null : e.getKey();
	}

	/**
	 * Returns the key corresponding to the specified Entry.
	 * 
	 * @throws NoSuchElementException
	 *           if the Entry is null
	 */
	static <K> K key(OMVRBTreeEntry<K, ?> e) {
		if (e == null)
			throw new NoSuchElementException();
		return e.getKey();
	}

	// SubMaps

	/**
	 * @serial include
	 */
	static abstract class NavigableSubMap<K, V> extends AbstractMap<K, V> implements ONavigableMap<K, V>, java.io.Serializable {
		/**
		 * The backing map.
		 */
		final OMVRBTree<K, V>	m;

		/**
		 * Endpoints are represented as triples (fromStart, lo, loInclusive) and (toEnd, hi, hiInclusive). If fromStart is true, then
		 * the low (absolute) bound is the start of the backing map, and the other values are ignored. Otherwise, if loInclusive is
		 * true, lo is the inclusive bound, else lo is the exclusive bound. Similarly for the upper bound.
		 */
		final K								lo, hi;
		final boolean					fromStart, toEnd;
		final boolean					loInclusive, hiInclusive;

		NavigableSubMap(final OMVRBTree<K, V> m, final boolean fromStart, K lo, final boolean loInclusive, final boolean toEnd, K hi,
				final boolean hiInclusive) {
			if (!fromStart && !toEnd) {
				if (m.compare(lo, hi) > 0)
					throw new IllegalArgumentException("fromKey > toKey");
			} else {
				if (!fromStart) // type check
					m.compare(lo, lo);
				if (!toEnd)
					m.compare(hi, hi);
			}

			this.m = m;
			this.fromStart = fromStart;
			this.lo = lo;
			this.loInclusive = loInclusive;
			this.toEnd = toEnd;
			this.hi = hi;
			this.hiInclusive = hiInclusive;
		}

		// internal utilities

		final boolean tooLow(final Object key) {
			if (!fromStart) {
				int c = m.compare(key, lo);
				if (c < 0 || (c == 0 && !loInclusive))
					return true;
			}
			return false;
		}

		final boolean tooHigh(final Object key) {
			if (!toEnd) {
				int c = m.compare(key, hi);
				if (c > 0 || (c == 0 && !hiInclusive))
					return true;
			}
			return false;
		}

		final boolean inRange(final Object key) {
			return !tooLow(key) && !tooHigh(key);
		}

		final boolean inClosedRange(final Object key) {
			return (fromStart || m.compare(key, lo) >= 0) && (toEnd || m.compare(hi, key) >= 0);
		}

		final boolean inRange(final Object key, final boolean inclusive) {
			return inclusive ? inRange(key) : inClosedRange(key);
		}

		/*
		 * Absolute versions of relation operations. Subclasses map to these using like-named "sub" versions that invert senses for
		 * descending maps
		 */

		final OMVRBTreeEntryPosition<K, V> absLowest() {
			OMVRBTreeEntry<K, V> e = (fromStart ? m.getFirstEntry() : (loInclusive ? m.getCeilingEntry(lo,
					PartialSearchMode.LOWEST_BOUNDARY) : m.getHigherEntry(lo)));
			return (e == null || tooHigh(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		final OMVRBTreeEntryPosition<K, V> absHighest() {
			OMVRBTreeEntry<K, V> e = (toEnd ? m.getLastEntry() : (hiInclusive ? m.getFloorEntry(hi, PartialSearchMode.HIGHEST_BOUNDARY)
					: m.getLowerEntry(hi)));
			return (e == null || tooLow(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		final OMVRBTreeEntryPosition<K, V> absCeiling(K key) {
			if (tooLow(key))
				return absLowest();
			OMVRBTreeEntry<K, V> e = m.getCeilingEntry(key, PartialSearchMode.NONE);
			return (e == null || tooHigh(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		final OMVRBTreeEntryPosition<K, V> absHigher(K key) {
			if (tooLow(key))
				return absLowest();
			OMVRBTreeEntry<K, V> e = m.getHigherEntry(key);
			return (e == null || tooHigh(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		final OMVRBTreeEntryPosition<K, V> absFloor(K key) {
			if (tooHigh(key))
				return absHighest();
			OMVRBTreeEntry<K, V> e = m.getFloorEntry(key, PartialSearchMode.NONE);
			return (e == null || tooLow(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		final OMVRBTreeEntryPosition<K, V> absLower(K key) {
			if (tooHigh(key))
				return absHighest();
			OMVRBTreeEntry<K, V> e = m.getLowerEntry(key);
			return (e == null || tooLow(e.getKey())) ? null : new OMVRBTreeEntryPosition<K, V>(e);
		}

		/** Returns the absolute high fence for ascending traversal */
		final OMVRBTreeEntryPosition<K, V> absHighFence() {
			return (toEnd ? null : new OMVRBTreeEntryPosition<K, V>(hiInclusive ? m.getHigherEntry(hi) : m.getCeilingEntry(hi,
					PartialSearchMode.LOWEST_BOUNDARY)));
		}

		/** Return the absolute low fence for descending traversal */
		final OMVRBTreeEntryPosition<K, V> absLowFence() {
			return (fromStart ? null : new OMVRBTreeEntryPosition<K, V>(loInclusive ? m.getLowerEntry(lo) : m.getFloorEntry(lo,
					PartialSearchMode.HIGHEST_BOUNDARY)));
		}

		// Abstract methods defined in ascending vs descending classes
		// These relay to the appropriate absolute versions

		abstract OMVRBTreeEntry<K, V> subLowest();

		abstract OMVRBTreeEntry<K, V> subHighest();

		abstract OMVRBTreeEntry<K, V> subCeiling(K key);

		abstract OMVRBTreeEntry<K, V> subHigher(K key);

		abstract OMVRBTreeEntry<K, V> subFloor(K key);

		abstract OMVRBTreeEntry<K, V> subLower(K key);

		/** Returns ascending iterator from the perspective of this submap */
		abstract OLazyIterator<K> keyIterator();

		/** Returns descending iterator from the perspective of this submap */
		abstract OLazyIterator<K> descendingKeyIterator();

		// public methods

		@Override
		public boolean isEmpty() {
			return (fromStart && toEnd) ? m.isEmpty() : entrySet().isEmpty();
		}

		@Override
		public int size() {
			return (fromStart && toEnd) ? m.size() : entrySet().size();
		}

		@Override
		public final boolean containsKey(Object key) {
			return inRange(key) && m.containsKey(key);
		}

		@Override
		public final V put(K key, V value) {
			if (!inRange(key))
				throw new IllegalArgumentException("key out of range");
			return m.put(key, value);
		}

		@Override
		public final V get(Object key) {
			return !inRange(key) ? null : m.get(key);
		}

		@Override
		public final V remove(Object key) {
			return !inRange(key) ? null : m.remove(key);
		}

		public final Map.Entry<K, V> ceilingEntry(K key) {
			return exportEntry(subCeiling(key));
		}

		public final K ceilingKey(K key) {
			return keyOrNull(subCeiling(key));
		}

		public final Map.Entry<K, V> higherEntry(K key) {
			return exportEntry(subHigher(key));
		}

		public final K higherKey(K key) {
			return keyOrNull(subHigher(key));
		}

		public final Map.Entry<K, V> floorEntry(K key) {
			return exportEntry(subFloor(key));
		}

		public final K floorKey(K key) {
			return keyOrNull(subFloor(key));
		}

		public final Map.Entry<K, V> lowerEntry(K key) {
			return exportEntry(subLower(key));
		}

		public final K lowerKey(K key) {
			return keyOrNull(subLower(key));
		}

		public final K firstKey() {
			return key(subLowest());
		}

		public final K lastKey() {
			return key(subHighest());
		}

		public final Map.Entry<K, V> firstEntry() {
			return exportEntry(subLowest());
		}

		public final Map.Entry<K, V> lastEntry() {
			return exportEntry(subHighest());
		}

		public final Map.Entry<K, V> pollFirstEntry() {
			OMVRBTreeEntry<K, V> e = subLowest();
			Map.Entry<K, V> result = exportEntry(e);
			if (e != null)
				m.deleteEntry(e);
			return result;
		}

		public final Map.Entry<K, V> pollLastEntry() {
			OMVRBTreeEntry<K, V> e = subHighest();
			Map.Entry<K, V> result = exportEntry(e);
			if (e != null)
				m.deleteEntry(e);
			return result;
		}

		// Views
		transient ONavigableMap<K, V>	descendingMapView		= null;
		transient EntrySetView				entrySetView				= null;
		transient KeySet<K>						navigableKeySetView	= null;

		@SuppressWarnings("rawtypes")
		public final ONavigableSet<K> navigableKeySet() {
			KeySet<K> nksv = navigableKeySetView;
			return (nksv != null) ? nksv : (navigableKeySetView = new OMVRBTree.KeySet(this));
		}

		@Override
		public final Set<K> keySet() {
			return navigableKeySet();
		}

		public ONavigableSet<K> descendingKeySet() {
			return descendingMap().navigableKeySet();
		}

		public final SortedMap<K, V> subMap(final K fromKey, final K toKey) {
			return subMap(fromKey, true, toKey, false);
		}

		public final SortedMap<K, V> headMap(final K toKey) {
			return headMap(toKey, false);
		}

		public final SortedMap<K, V> tailMap(final K fromKey) {
			return tailMap(fromKey, true);
		}

		// View classes

		abstract class EntrySetView extends AbstractSet<Map.Entry<K, V>> {
			private transient int	size	= -1, sizeModCount;

			@Override
			public int size() {
				if (fromStart && toEnd)
					return m.size();
				if (size == -1 || sizeModCount != m.modCount) {
					sizeModCount = m.modCount;
					size = 0;
					Iterator<?> i = iterator();
					while (i.hasNext()) {
						size++;
						i.next();
					}
				}
				return size;
			}

			@Override
			public boolean isEmpty() {
				OMVRBTreeEntryPosition<K, V> n = absLowest();
				return n == null || tooHigh(n.getKey());
			}

			@Override
			public boolean contains(final Object o) {
				if (!(o instanceof OMVRBTreeEntry))
					return false;
				final OMVRBTreeEntry<K, V> entry = (OMVRBTreeEntry<K, V>) o;
				final K key = entry.getKey();
				if (!inRange(key))
					return false;
				V nodeValue = m.get(key);
				return nodeValue != null && valEquals(nodeValue, entry.getValue());
			}

			@Override
			public boolean remove(final Object o) {
				if (!(o instanceof OMVRBTreeEntry))
					return false;
				final OMVRBTreeEntry<K, V> entry = (OMVRBTreeEntry<K, V>) o;
				final K key = entry.getKey();
				if (!inRange(key))
					return false;
				final OMVRBTreeEntry<K, V> node = m.getEntry(key, PartialSearchMode.NONE);
				if (node != null && valEquals(node.getValue(), entry.getValue())) {
					m.deleteEntry(node);
					return true;
				}
				return false;
			}
		}

		/**
		 * Iterators for SubMaps
		 */
		abstract class SubMapIterator<T> implements OLazyIterator<T> {
			OMVRBTreeEntryPosition<K, V>	lastReturned;
			OMVRBTreeEntryPosition<K, V>	next;
			final K												fenceKey;
			int														expectedModCount;

			SubMapIterator(final OMVRBTreeEntryPosition<K, V> first, final OMVRBTreeEntryPosition<K, V> fence) {
				expectedModCount = m.modCount;
				lastReturned = null;
				next = first;
				fenceKey = fence == null ? null : fence.getKey();
			}

			public final boolean hasNext() {
				if (next != null) {
					final K k = next.getKey();
					return k != fenceKey && !k.equals(fenceKey);
				}
				return false;
			}

			final OMVRBTreeEntryPosition<K, V> nextEntry() {
				final OMVRBTreeEntryPosition<K, V> e;
				if (next != null)
					e = new OMVRBTreeEntryPosition<K, V>(next);
				else
					e = null;
				final K k = e.getKey();
				if (e == null || e.entry == null || k == fenceKey || k.equals(fenceKey))
					throw new NoSuchElementException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				next.assign(OMVRBTree.next(e));
				lastReturned = e;
				return e;
			}

			final OMVRBTreeEntryPosition<K, V> prevEntry() {
				final OMVRBTreeEntryPosition<K, V> e;
				if (next != null)
					e = new OMVRBTreeEntryPosition<K, V>(next);
				else
					e = null;
				final K k = e.getKey();
				if (e == null || e.entry == null || k == fenceKey || k.equals(fenceKey))
					throw new NoSuchElementException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				next.assign(OMVRBTree.previous(e));
				lastReturned = e;
				return e;
			}

			final public T update(final T iValue) {
				if (lastReturned == null)
					throw new IllegalStateException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				return (T) lastReturned.entry.setValue((V) iValue);
			}

			final void removeAscending() {
				if (lastReturned == null)
					throw new IllegalStateException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				// deleted entries are replaced by their successors
				if (lastReturned.entry.getLeft() != null && lastReturned.entry.getRight() != null)
					next = lastReturned;
				m.deleteEntry(lastReturned.entry);
				lastReturned = null;
				expectedModCount = m.modCount;
			}

			final void removeDescending() {
				if (lastReturned == null)
					throw new IllegalStateException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				m.deleteEntry(lastReturned.entry);
				lastReturned = null;
				expectedModCount = m.modCount;
			}

		}

		final class SubMapEntryIterator extends SubMapIterator<Map.Entry<K, V>> {
			SubMapEntryIterator(final OMVRBTreeEntryPosition<K, V> first, final OMVRBTreeEntryPosition<K, V> fence) {
				super(first, fence);
			}

			public Map.Entry<K, V> next() {
				final Map.Entry<K, V> e = OMVRBTree.exportEntry(next);
				nextEntry();
				return e;
			}

			public void remove() {
				removeAscending();
			}
		}

		final class SubMapKeyIterator extends SubMapIterator<K> {
			SubMapKeyIterator(final OMVRBTreeEntryPosition<K, V> first, final OMVRBTreeEntryPosition<K, V> fence) {
				super(first, fence);
			}

			public K next() {
				return nextEntry().getKey();
			}

			public void remove() {
				removeAscending();
			}
		}

		final class DescendingSubMapEntryIterator extends SubMapIterator<Map.Entry<K, V>> {
			DescendingSubMapEntryIterator(final OMVRBTreeEntryPosition<K, V> last, final OMVRBTreeEntryPosition<K, V> fence) {
				super(last, fence);
			}

			public Map.Entry<K, V> next() {
				final Map.Entry<K, V> e = OMVRBTree.exportEntry(next);
				prevEntry();
				return e;
			}

			public void remove() {
				removeDescending();
			}
		}

		final class DescendingSubMapKeyIterator extends SubMapIterator<K> {
			DescendingSubMapKeyIterator(final OMVRBTreeEntryPosition<K, V> last, final OMVRBTreeEntryPosition<K, V> fence) {
				super(last, fence);
			}

			public K next() {
				return prevEntry().getKey();
			}

			public void remove() {
				removeDescending();
			}
		}
	}

	/**
	 * @serial include
	 */
	static final class AscendingSubMap<K, V> extends NavigableSubMap<K, V> {
		private static final long	serialVersionUID	= 912986545866124060L;

		AscendingSubMap(final OMVRBTree<K, V> m, final boolean fromStart, final K lo, final boolean loInclusive, final boolean toEnd,
				K hi, final boolean hiInclusive) {
			super(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive);
		}

		public Comparator<? super K> comparator() {
			return m.comparator();
		}

		public ONavigableMap<K, V> subMap(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive) {
			if (!inRange(fromKey, fromInclusive))
				throw new IllegalArgumentException("fromKey out of range");
			if (!inRange(toKey, toInclusive))
				throw new IllegalArgumentException("toKey out of range");
			return new AscendingSubMap<K, V>(m, false, fromKey, fromInclusive, false, toKey, toInclusive);
		}

		public ONavigableMap<K, V> headMap(final K toKey, final boolean inclusive) {
			if (!inRange(toKey, inclusive))
				throw new IllegalArgumentException("toKey out of range");
			return new AscendingSubMap<K, V>(m, fromStart, lo, loInclusive, false, toKey, inclusive);
		}

		public ONavigableMap<K, V> tailMap(final K fromKey, final boolean inclusive) {
			if (!inRange(fromKey, inclusive))
				throw new IllegalArgumentException("fromKey out of range");
			return new AscendingSubMap<K, V>(m, false, fromKey, inclusive, toEnd, hi, hiInclusive);
		}

		public ONavigableMap<K, V> descendingMap() {
			ONavigableMap<K, V> mv = descendingMapView;
			return (mv != null) ? mv : (descendingMapView = new DescendingSubMap<K, V>(m, fromStart, lo, loInclusive, toEnd, hi,
					hiInclusive));
		}

		@Override
		OLazyIterator<K> keyIterator() {
			return new SubMapKeyIterator(absLowest(), absHighFence());
		}

		@Override
		OLazyIterator<K> descendingKeyIterator() {
			return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
		}

		final class AscendingEntrySetView extends EntrySetView {
			@Override
			public Iterator<Map.Entry<K, V>> iterator() {
				return new SubMapEntryIterator(absLowest(), absHighFence());
			}
		}

		@Override
		public Set<Map.Entry<K, V>> entrySet() {
			EntrySetView es = entrySetView;
			return (es != null) ? es : new AscendingEntrySetView();
		}

		@Override
		OMVRBTreeEntry<K, V> subLowest() {
			return absLowest().entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subHighest() {
			return absHighest().entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subCeiling(final K key) {
			return absCeiling(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subHigher(final K key) {
			return absHigher(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subFloor(final K key) {
			return absFloor(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subLower(final K key) {
			return absLower(key).entry;
		}
	}

	/**
	 * @serial include
	 */
	static final class DescendingSubMap<K, V> extends NavigableSubMap<K, V> {
		private static final long						serialVersionUID	= 912986545866120460L;

		private final Comparator<? super K>	reverseComparator	= Collections.reverseOrder(m.comparator);

		DescendingSubMap(final OMVRBTree<K, V> m, final boolean fromStart, final K lo, final boolean loInclusive, final boolean toEnd,
				final K hi, final boolean hiInclusive) {
			super(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive);
		}

		public Comparator<? super K> comparator() {
			return reverseComparator;
		}

		public ONavigableMap<K, V> subMap(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive) {
			if (!inRange(fromKey, fromInclusive))
				throw new IllegalArgumentException("fromKey out of range");
			if (!inRange(toKey, toInclusive))
				throw new IllegalArgumentException("toKey out of range");
			return new DescendingSubMap<K, V>(m, false, toKey, toInclusive, false, fromKey, fromInclusive);
		}

		public ONavigableMap<K, V> headMap(final K toKey, final boolean inclusive) {
			if (!inRange(toKey, inclusive))
				throw new IllegalArgumentException("toKey out of range");
			return new DescendingSubMap<K, V>(m, false, toKey, inclusive, toEnd, hi, hiInclusive);
		}

		public ONavigableMap<K, V> tailMap(final K fromKey, final boolean inclusive) {
			if (!inRange(fromKey, inclusive))
				throw new IllegalArgumentException("fromKey out of range");
			return new DescendingSubMap<K, V>(m, fromStart, lo, loInclusive, false, fromKey, inclusive);
		}

		public ONavigableMap<K, V> descendingMap() {
			ONavigableMap<K, V> mv = descendingMapView;
			return (mv != null) ? mv : (descendingMapView = new AscendingSubMap<K, V>(m, fromStart, lo, loInclusive, toEnd, hi,
					hiInclusive));
		}

		@Override
		OLazyIterator<K> keyIterator() {
			return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
		}

		@Override
		OLazyIterator<K> descendingKeyIterator() {
			return new SubMapKeyIterator(absLowest(), absHighFence());
		}

		final class DescendingEntrySetView extends EntrySetView {
			@Override
			public Iterator<Map.Entry<K, V>> iterator() {
				return new DescendingSubMapEntryIterator(absHighest(), absLowFence());
			}
		}

		@Override
		public Set<Map.Entry<K, V>> entrySet() {
			EntrySetView es = entrySetView;
			return (es != null) ? es : new DescendingEntrySetView();
		}

		@Override
		OMVRBTreeEntry<K, V> subLowest() {
			return absHighest().entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subHighest() {
			return absLowest().entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subCeiling(final K key) {
			return absFloor(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subHigher(final K key) {
			return absLower(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subFloor(final K key) {
			return absCeiling(key).entry;
		}

		@Override
		OMVRBTreeEntry<K, V> subLower(final K key) {
			return absHigher(key).entry;
		}
	}

	// Red-black mechanics

	public static final boolean	RED		= false;
	public static final boolean	BLACK	= true;

	/**
	 * Node in the Tree. Doubles as a means to pass key-value pairs back to user (see Map.Entry).
	 */

	/**
	 * Returns the first Entry in the OMVRBTree (according to the OMVRBTree's key-sort function). Returns null if the OMVRBTree is
	 * empty.
	 */
	protected OMVRBTreeEntry<K, V> getFirstEntry() {
		OMVRBTreeEntry<K, V> p = root;
		if (p != null) {
			if (p.getSize() > 0)
				pageIndex = 0;

			while (p.getLeft() != null)
				p = p.getLeft();
		}
		return p;
	}

	/**
	 * Returns the last Entry in the OMVRBTree (according to the OMVRBTree's key-sort function). Returns null if the OMVRBTree is
	 * empty.
	 */
	protected OMVRBTreeEntry<K, V> getLastEntry() {
		OMVRBTreeEntry<K, V> p = root;
		if (p != null)
			while (p.getRight() != null)
				p = p.getRight();

		if (p != null)
			pageIndex = p.getSize() - 1;

		return p;
	}

	public static <K, V> OMVRBTreeEntry<K, V> successor(final OMVRBTreeEntryPosition<K, V> t) {
		t.entry.getTree().setPageIndex(t.position);
		return successor(t.entry);
	}

	/**
	 * Returns the successor of the specified Entry, or null if no such.
	 */
	public static <K, V> OMVRBTreeEntry<K, V> successor(final OMVRBTreeEntry<K, V> t) {
		if (t == null)
			return null;

		OMVRBTreeEntry<K, V> p = null;

		if (t.getRight() != null) {
			p = t.getRight();
			while (p.getLeft() != null)
				p = p.getLeft();
		} else {
			p = t.getParent();
			OMVRBTreeEntry<K, V> ch = t;
			while (p != null && ch == p.getRight()) {
				ch = p;
				p = p.getParent();
			}
		}

		return p;
	}

	public static <K, V> OMVRBTreeEntry<K, V> next(final OMVRBTreeEntryPosition<K, V> t) {
		t.entry.getTree().setPageIndex(t.position);
		return next(t.entry);
	}

	/**
	 * Returns the next item of the tree.
	 */
	public static <K, V> OMVRBTreeEntry<K, V> next(final OMVRBTreeEntry<K, V> t) {
		if (t == null)
			return null;

		final OMVRBTreeEntry<K, V> succ;
		if (t.tree.pageIndex < t.getSize() - 1) {
			// ITERATE INSIDE THE NODE
			succ = t;
			t.tree.pageIndex++;
		} else {
			// GET THE NEXT NODE
			succ = OMVRBTree.successor(t);
			t.tree.pageIndex = 0;
		}

		return succ;
	}

	public static <K, V> OMVRBTreeEntry<K, V> predecessor(final OMVRBTreeEntryPosition<K, V> t) {
		t.entry.getTree().setPageIndex(t.position);
		return predecessor(t.entry);
	}

	/**
	 * Returns the predecessor of the specified Entry, or null if no such.
	 */
	public static <K, V> OMVRBTreeEntry<K, V> predecessor(final OMVRBTreeEntry<K, V> t) {
		if (t == null)
			return null;
		else if (t.getLeft() != null) {
			OMVRBTreeEntry<K, V> p = t.getLeft();
			while (p.getRight() != null)
				p = p.getRight();
			return p;
		} else {
			OMVRBTreeEntry<K, V> p = t.getParent();
			Entry<K, V> ch = t;
			while (p != null && ch == p.getLeft()) {
				ch = p;
				p = p.getParent();
			}
			return p;
		}
	}

	public static <K, V> OMVRBTreeEntry<K, V> previous(final OMVRBTreeEntryPosition<K, V> t) {
		t.entry.getTree().setPageIndex(t.position);
		return previous(t.entry);
	}

	/**
	 * Returns the previous item of the tree.
	 */
	public static <K, V> OMVRBTreeEntry<K, V> previous(final OMVRBTreeEntry<K, V> t) {
		if (t == null)
			return null;

		final int index = t.getTree().getPageIndex();

		final OMVRBTreeEntry<K, V> prev;
		if (index <= 0) {
			prev = predecessor(t);
			if (prev != null)
				t.tree.pageIndex = prev.getSize() - 1;
			else
				t.tree.pageIndex = 0;
		} else {
			prev = t;
			t.tree.pageIndex = index - 1;
		}

		return prev;
	}

	/**
	 * Balancing operations.
	 * 
	 * Implementations of rebalancings during insertion and deletion are slightly different than the CLR version. Rather than using
	 * dummy nilnodes, we use a set of accessors that deal properly with null. They are used to avoid messiness surrounding nullness
	 * checks in the main algorithms.
	 */

	private static <K, V> boolean colorOf(final OMVRBTreeEntry<K, V> p) {
		return (p == null ? BLACK : p.getColor());
	}

	private static <K, V> OMVRBTreeEntry<K, V> parentOf(final OMVRBTreeEntry<K, V> p) {
		return (p == null ? null : p.getParent());
	}

	private static <K, V> void setColor(final OMVRBTreeEntry<K, V> p, final boolean c) {
		if (p != null)
			p.setColor(c);
	}

	private static <K, V> OMVRBTreeEntry<K, V> leftOf(final OMVRBTreeEntry<K, V> p) {
		return (p == null) ? null : p.getLeft();
	}

	private static <K, V> OMVRBTreeEntry<K, V> rightOf(final OMVRBTreeEntry<K, V> p) {
		return (p == null) ? null : p.getRight();
	}

	/** From CLR */
	protected void rotateLeft(final OMVRBTreeEntry<K, V> p) {
		if (p != null) {
			OMVRBTreeEntry<K, V> r = p.getRight();
			p.setRight(r.getLeft());
			if (r.getLeft() != null)
				r.getLeft().setParent(p);
			r.setParent(p.getParent());
			if (p.getParent() == null)
				setRoot(r);
			else if (p.getParent().getLeft() == p)
				p.getParent().setLeft(r);
			else
				p.getParent().setRight(r);
			p.setParent(r);
			r.setLeft(p);
		}
	}

	protected void setRoot(final OMVRBTreeEntry<K, V> iRoot) {
		root = iRoot;
	}

	/** From CLR */
	protected void rotateRight(final OMVRBTreeEntry<K, V> p) {
		if (p != null) {
			OMVRBTreeEntry<K, V> l = p.getLeft();
			p.setLeft(l.getRight());
			if (l.getRight() != null)
				l.getRight().setParent(p);
			l.setParent(p.getParent());
			if (p.getParent() == null)
				setRoot(l);
			else if (p.getParent().getRight() == p)
				p.getParent().setRight(l);
			else
				p.getParent().setLeft(l);
			l.setRight(p);
			p.setParent(l);
		}
	}

	private OMVRBTreeEntry<K, V> grandparent(final OMVRBTreeEntry<K, V> n) {
		return parentOf(parentOf(n));
	}

	private OMVRBTreeEntry<K, V> uncle(final OMVRBTreeEntry<K, V> n) {
		if (parentOf(n) == leftOf(grandparent(n)))
			return rightOf(grandparent(n));
		else
			return leftOf(grandparent(n));
	}

	private void fixAfterInsertion(final OMVRBTreeEntry<K, V> n) {
		if (parentOf(n) == null)
			setColor(n, BLACK);
		else
			insert_case2(n);
	}

	private void insert_case2(final OMVRBTreeEntry<K, V> n) {
		if (colorOf(parentOf(n)) == BLACK)
			return; /* Tree is still valid */
		else
			insert_case3(n);
	}

	private void insert_case3(final OMVRBTreeEntry<K, V> n) {
		if (uncle(n) != null && colorOf(uncle(n)) == RED) {
			setColor(parentOf(n), BLACK);
			setColor(uncle(n), BLACK);
			setColor(grandparent(n), RED);
			fixAfterInsertion(grandparent(n));
		} else
			insert_case4(n);
	}

	private void insert_case4(OMVRBTreeEntry<K, V> n) {
		if (n == rightOf(parentOf(n)) && parentOf(n) == leftOf(grandparent(n))) {
			rotateLeft(parentOf(n));
			n = leftOf(n);
		} else if (n == leftOf(parentOf(n)) && parentOf(n) == rightOf(grandparent(n))) {
			rotateRight(parentOf(n));
			n = rightOf(n);
		}
		insert_case5(n);
	}

	private void insert_case5(final OMVRBTreeEntry<K, V> n) {
		setColor(parentOf(n), BLACK);
		setColor(grandparent(n), RED);
		if (n == leftOf(parentOf(n)) && parentOf(n) == leftOf(grandparent(n))) {
			rotateRight(grandparent(n));
		} else {
			rotateLeft(grandparent(n));
		}
	}

	/**
	 * Delete node p, and then re-balance the tree.
	 * 
	 * @param p
	 *          node to delete
	 * @return
	 */
	OMVRBTreeEntry<K, V> deleteEntry(OMVRBTreeEntry<K, V> p) {
		setSizeDelta(-1);
		modCount++;
		if (pageIndex > -1) {
			// DELETE INSIDE THE NODE
			p.remove();

			if (p.getSize() > 0)
				return p;
		}

		final OMVRBTreeEntry<K, V> next = successor(p);
		// DELETE THE ENTIRE NODE, RE-BUILDING THE STRUCTURE
		removeNode(p);

		// RETURN NEXT NODE
		return next;
	}

	/**
	 * Remove a node from the tree.
	 * 
	 * @param p
	 *          Node to remove
	 */
	protected void removeNode(OMVRBTreeEntry<K, V> p) {
		modCount++;
		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p.getLeft() != null && p.getRight() != null) {
			OMVRBTreeEntry<K, V> s = next(p);
			p.copyFrom(s);
			p = s;
		} // p has 2 children

		// Start fixup at replacement node, if it exists.
		final OMVRBTreeEntry<K, V> replacement = (p.getLeft() != null ? p.getLeft() : p.getRight());

		if (replacement != null) {
			// Link replacement to parent
			replacement.setParent(p.getParent());
			if (p.getParent() == null)
				setRoot(replacement);
			else if (p == p.getParent().getLeft())
				p.getParent().setLeft(replacement);
			else
				p.getParent().setRight(replacement);

			// Null out links so they are OK to use by fixAfterDeletion.
			p.setLeft(null);
			p.setRight(null);
			p.setParent(null);

			// Fix replacement
			if (p.getColor() == BLACK)
				fixAfterDeletion(replacement);
		} else if (p.getParent() == null) { // return if we are the only node.
			clear();
		} else { // No children. Use self as phantom replacement and unlink.
			if (p.getColor() == BLACK)
				fixAfterDeletion(p);

			if (p.getParent() != null) {
				if (p == p.getParent().getLeft())
					p.getParent().setLeft(null);
				else if (p == p.getParent().getRight())
					p.getParent().setRight(null);
				p.setParent(null);
			}
		}
	}

	/** From CLR */
	private void fixAfterDeletion(OMVRBTreeEntry<K, V> x) {
		while (x != root && colorOf(x) == BLACK) {
			if (x == leftOf(parentOf(x))) {
				OMVRBTreeEntry<K, V> sib = rightOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateLeft(parentOf(x));
					sib = rightOf(parentOf(x));
				}

				if (colorOf(leftOf(sib)) == BLACK && colorOf(rightOf(sib)) == BLACK) {
					setColor(sib, RED);
					x = parentOf(x);
				} else {
					if (colorOf(rightOf(sib)) == BLACK) {
						setColor(leftOf(sib), BLACK);
						setColor(sib, RED);
						rotateRight(sib);
						sib = rightOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(rightOf(sib), BLACK);
					rotateLeft(parentOf(x));
					x = root;
				}
			} else { // symmetric
				OMVRBTreeEntry<K, V> sib = leftOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateRight(parentOf(x));
					sib = leftOf(parentOf(x));
				}

				if (x != null && colorOf(rightOf(sib)) == BLACK && colorOf(leftOf(sib)) == BLACK) {
					setColor(sib, RED);
					x = parentOf(x);
				} else {
					if (colorOf(leftOf(sib)) == BLACK) {
						setColor(rightOf(sib), BLACK);
						setColor(sib, RED);
						rotateLeft(sib);
						sib = leftOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(leftOf(sib), BLACK);
					rotateRight(parentOf(x));
					x = root;
				}
			}
		}

		setColor(x, BLACK);
	}

	/**
	 * Save the state of the <tt>OMVRBTree</tt> instance to a stream (i.e., serialize it).
	 * 
	 * @serialData The <i>size</i> of the OMVRBTree (the number of key-value mappings) is emitted (int), followed by the key (Object)
	 *             and value (Object) for each key-value mapping represented by the OMVRBTree. The key-value mappings are emitted in
	 *             key-order (as determined by the OMVRBTree's Comparator, or by the keys' natural ordering if the OMVRBTree has no
	 *             Comparator).
	 */
	private void writeObject(final ObjectOutputStream s) throws java.io.IOException {
		// Write out the Comparator and any hidden stuff
		s.defaultWriteObject();

		// Write out size (number of Mappings)
		s.writeInt(size());

		// Write out keys and values (alternating)
		for (Iterator<Map.Entry<K, V>> i = entrySet().iterator(); i.hasNext();) {
			Entry<K, V> e = i.next();
			s.writeObject(e.getKey());
			s.writeObject(e.getValue());
		}
	}

	/**
	 * Reconstitute the <tt>OMVRBTree</tt> instance from a stream (i.e., deserialize it).
	 */
	private void readObject(final java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
		// Read in the Comparator and any hidden stuff
		s.defaultReadObject();

		// Read in size
		setSize(s.readInt());

		buildFromSorted(size(), null, s, null);
	}

	/** Intended to be called only from OTreeSet.readObject */
	void readOTreeSet(int iSize, ObjectInputStream s, V defaultVal) throws java.io.IOException, ClassNotFoundException {
		buildFromSorted(iSize, null, s, defaultVal);
	}

	/** Intended to be called only from OTreeSet.addAll */
	void addAllForOTreeSet(SortedSet<? extends K> set, V defaultVal) {
		try {
			buildFromSorted(set.size(), set.iterator(), null, defaultVal);
		} catch (java.io.IOException cannotHappen) {
		} catch (ClassNotFoundException cannotHappen) {
		}
	}

	/**
	 * Linear time tree building algorithm from sorted data. Can accept keys and/or values from iterator or stream. This leads to too
	 * many parameters, but seems better than alternatives. The four formats that this method accepts are:
	 * 
	 * 1) An iterator of Map.Entries. (it != null, defaultVal == null). 2) An iterator of keys. (it != null, defaultVal != null). 3) A
	 * stream of alternating serialized keys and values. (it == null, defaultVal == null). 4) A stream of serialized keys. (it ==
	 * null, defaultVal != null).
	 * 
	 * It is assumed that the comparator of the OMVRBTree is already set prior to calling this method.
	 * 
	 * @param size
	 *          the number of keys (or key-value pairs) to be read from the iterator or stream
	 * @param it
	 *          If non-null, new entries are created from entries or keys read from this iterator.
	 * @param str
	 *          If non-null, new entries are created from keys and possibly values read from this stream in serialized form. Exactly
	 *          one of it and str should be non-null.
	 * @param defaultVal
	 *          if non-null, this default value is used for each value in the map. If null, each value is read from iterator or
	 *          stream, as described above.
	 * @throws IOException
	 *           propagated from stream reads. This cannot occur if str is null.
	 * @throws ClassNotFoundException
	 *           propagated from readObject. This cannot occur if str is null.
	 */
	private void buildFromSorted(final int size, final Iterator<?> it, final java.io.ObjectInputStream str, final V defaultVal)
			throws java.io.IOException, ClassNotFoundException {
		setSize(size);
		root = buildFromSorted(0, 0, size - 1, computeRedLevel(size), it, str, defaultVal);
	}

	/**
	 * Recursive "helper method" that does the real work of the previous method. Identically named parameters have identical
	 * definitions. Additional parameters are documented below. It is assumed that the comparator and size fields of the OMVRBTree are
	 * already set prior to calling this method. (It ignores both fields.)
	 * 
	 * @param level
	 *          the current level of tree. Initial call should be 0.
	 * @param lo
	 *          the first element index of this subtree. Initial should be 0.
	 * @param hi
	 *          the last element index of this subtree. Initial should be size-1.
	 * @param redLevel
	 *          the level at which nodes should be red. Must be equal to computeRedLevel for tree of this size.
	 */
	private final OMVRBTreeEntry<K, V> buildFromSorted(final int level, final int lo, final int hi, final int redLevel,
			final Iterator<?> it, final java.io.ObjectInputStream str, final V defaultVal) throws java.io.IOException,
			ClassNotFoundException {
		/*
		 * Strategy: The root is the middlemost element. To get to it, we have to first recursively construct the entire left subtree,
		 * so as to grab all of its elements. We can then proceed with right subtree.
		 * 
		 * The lo and hi arguments are the minimum and maximum indices to pull out of the iterator or stream for current subtree. They
		 * are not actually indexed, we just proceed sequentially, ensuring that items are extracted in corresponding order.
		 */

		if (hi < lo)
			return null;

		final int mid = (lo + hi) / 2;

		OMVRBTreeEntry<K, V> left = null;
		if (lo < mid)
			left = buildFromSorted(level + 1, lo, mid - 1, redLevel, it, str, defaultVal);

		// extract key and/or value from iterator or stream
		K key;
		V value;
		if (it != null) {
			if (defaultVal == null) {
				OMVRBTreeEntry<K, V> entry = (OMVRBTreeEntry<K, V>) it.next();
				key = entry.getKey();
				value = entry.getValue();
			} else {
				key = (K) it.next();
				value = defaultVal;
			}
		} else { // use stream
			key = (K) str.readObject();
			value = (defaultVal != null ? defaultVal : (V) str.readObject());
		}

		final OMVRBTreeEntry<K, V> middle = createEntry(key, value);

		// color nodes in non-full bottom most level red
		if (level == redLevel)
			middle.setColor(RED);

		if (left != null) {
			middle.setLeft(left);
			left.setParent(middle);
		}

		if (mid < hi) {
			OMVRBTreeEntry<K, V> right = buildFromSorted(level + 1, mid + 1, hi, redLevel, it, str, defaultVal);
			middle.setRight(right);
			right.setParent(middle);
		}

		return middle;
	}

	/**
	 * Find the level down to which to assign all nodes BLACK. This is the last `full' level of the complete binary tree produced by
	 * buildTree. The remaining nodes are colored RED. (This makes a `nice' set of color assignments wrt future insertions.) This
	 * level number is computed by finding the number of splits needed to reach the zeroeth node. (The answer is ~lg(N), but in any
	 * case must be computed by same quick O(lg(N)) loop.)
	 */
	private static int computeRedLevel(final int sz) {
		int level = 0;
		for (int m = sz - 1; m >= 0; m = m / 2 - 1)
			level++;
		return level;
	}

	public int getPageIndex() {
		return pageIndex;
	}

	public void setPageIndex(final int iPageIndex) {
		pageIndex = iPageIndex;
	}

	private void init() {
	}

	public OMVRBTreeEntry<K, V> getRoot() {
		return root;
	}

	protected void printInMemoryStructure(final OMVRBTreeEntry<K, V> iRootNode) {
		printInMemoryNode("root", iRootNode, 0);
	}

	private void printInMemoryNode(final String iLabel, OMVRBTreeEntry<K, V> iNode, int iLevel) {
		if (iNode == null)
			return;

		for (int i = 0; i < iLevel; ++i)
			System.out.print(' ');

		System.out.println(iLabel + ": " + iNode.toString() + " (" + (iNode.getColor() ? "B" : "R") + ")");

		++iLevel;

		printInMemoryNode(iLevel + ".left", iNode.getLeftInMemory(), iLevel);
		printInMemoryNode(iLevel + ".right", iNode.getRightInMemory(), iLevel);
	}

	public void checkTreeStructure(final OMVRBTreeEntry<K, V> iRootNode) {
		if (!runtimeCheckEnabled || iRootNode == null)
			return;

		int currPageIndex = pageIndex;

		OMVRBTreeEntry<K, V> prevNode = null;
		int i = 0;
		for (OMVRBTreeEntry<K, V> e = iRootNode.getFirstInMemory(); e != null; e = e.getNextInMemory()) {
			if (e.getSize() == 0)
				OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] Node %s has 0 items\n", e);

			if (prevNode != null) {
				if (prevNode.getTree() == null)
					OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] Freed record %d found in memory\n", i);

				if (((Comparable<K>) e.getFirstKey()).compareTo((e.getLastKey())) > 0) {
					OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] begin key is > than last key\n", e.getFirstKey(),
							e.getLastKey());
					printInMemoryStructure(iRootNode);
				}

				if (((Comparable<K>) e.getFirstKey()).compareTo((prevNode.getLastKey())) < 0) {
					OLogManager.instance().error(this,
							"[OMVRBTree.checkTreeStructure] Node %s starts with a key minor than the last key of the previous node %s\n", e,
							prevNode);
					printInMemoryStructure(e.getParentInMemory() != null ? e.getParentInMemory() : e);
				}
			}

			if (e.getLeftInMemory() != null && e.getLeftInMemory() == e) {
				OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] Node %s has left that points to itself!\n", e);
				printInMemoryStructure(iRootNode);
			}

			if (e.getRightInMemory() != null && e.getRightInMemory() == e) {
				OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] Node %s has right that points to itself!\n", e);
				printInMemoryStructure(iRootNode);
			}

			if (e.getLeftInMemory() != null && e.getLeftInMemory() == e.getRightInMemory()) {
				OLogManager.instance().error(this, "[OMVRBTree.checkTreeStructure] Node %s has left and right equals!\n", e);
				printInMemoryStructure(iRootNode);
			}

			if (e.getParentInMemory() != null && e.getParentInMemory().getRightInMemory() != e
					&& e.getParentInMemory().getLeftInMemory() != e) {
				OLogManager.instance().error(this,
						"[OMVRBTree.checkTreeStructure] Node %s is the children of node %s but the cross-reference is missed!\n", e,
						e.getParentInMemory());
				printInMemoryStructure(iRootNode);
			}

			prevNode = e;
			++i;
		}

		pageIndex = currPageIndex;
	}

	public boolean isRuntimeCheckEnabled() {
		return runtimeCheckEnabled;
	}

	public void setChecks(boolean checks) {
		this.runtimeCheckEnabled = checks;
	}

	public void setRuntimeCheckEnabled(boolean runtimeCheckEnabled) {
		this.runtimeCheckEnabled = runtimeCheckEnabled;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	protected OMVRBTreeEntry<K, V> getLastSearchNodeForSameKey(final Object key) {
		if (key != null && lastSearchKey != null)
			if (comparator != null)
				return comparator.compare((K) key, (K) lastSearchKey) == 0 ? lastSearchNode : null;
			else
				try {
					return ((Comparable<? super K>) key).compareTo((K) lastSearchKey) == 0 ? lastSearchNode : null;
				} catch (Exception e) {
					// IGNORE IT
				}

		return null;
	}

	protected OMVRBTreeEntry<K, V> setLastSearchNode(final Object iKey, final OMVRBTreeEntry<K, V> iNode) {
		lastSearchKey = iKey;
		lastSearchNode = iNode;
		lastSearchFound = iNode != null ? iNode.tree.pageItemFound : false;
		lastSearchIndex = iNode != null ? iNode.tree.pageIndex : -1;
		return iNode;
	}

	protected void searchNodeCallback() {
	}

	protected void setSizeDelta(final int iDelta) {
		setSize(size() + iDelta);
	}
}
