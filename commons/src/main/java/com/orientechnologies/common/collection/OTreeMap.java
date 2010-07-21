package com.orientechnologies.common.collection;

import java.io.IOException;
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

@SuppressWarnings("unchecked")
public abstract class OTreeMap<K, V> extends AbstractMap<K, V> implements ONavigableMap<K, V>, Cloneable, java.io.Serializable {
	OTreeMapEventListener<K, V>							listener;
	boolean																	pageItemFound		= false;
	volatile int														pageIndex				= -1;

	protected int														lastPageSize		= 63;		// PERSISTENT FIELDS

	/**
	 * The number of entries in the tree
	 */
	protected int														size						= 0;			// PERSISTENT FIELDS

	float																		pageLoadFactor	= 0.7f;

	/**
	 * The comparator used to maintain order in this tree map, or null if it uses the natural ordering of its keys.
	 * 
	 * @serial
	 */
	private final Comparator<? super K>			comparator;

	protected transient OTreeMapEntry<K, V>	root						= null;

	/**
	 * The number of structural modifications to the tree.
	 */
	transient int														modCount				= 0;

	public OTreeMap(final int iSize, final float iLoadFactor) {
		lastPageSize = iSize;
		pageLoadFactor = iLoadFactor;
		comparator = null;
		init();
	}

	public OTreeMap(final OTreeMapEventListener<K, V> iListener) {
		init();
		comparator = null;
		listener = iListener;
	}

	/**
	 * Constructs a new, empty tree map, using the natural ordering of its keys. All keys inserted into the map must implement the
	 * {@link Comparable} interface. Furthermore, all such keys must be <i>mutually comparable</i>: <tt>k1.compareTo(k2)</tt> must not
	 * throw a <tt>ClassCastException</tt> for any keys <tt>k1</tt> and <tt>k2</tt> in the map. If the user attempts to put a key into
	 * the map that violates this constraint (for example, the user attempts to put a string key into a map whose keys are integers),
	 * the <tt>put(Object key, Object value)</tt> call will throw a <tt>ClassCastException</tt>.
	 */
	public OTreeMap() {
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
	 * @param comparator
	 *          the comparator that will be used to order this map. If <tt>null</tt>, the {@linkplain Comparable natural ordering} of
	 *          the keys will be used.
	 */
	public OTreeMap(final Comparator<? super K> comparator) {
		init();
		this.comparator = comparator;
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
	public OTreeMap(final Map<? extends K, ? extends V> m) {
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
	public OTreeMap(final SortedMap<K, ? extends V> m) {
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
	protected abstract OTreeMapEntry<K, V> createEntry(final K key, final V value);

	/**
	 * Create a new node with the same parent of the node is splitting.
	 */
	protected abstract OTreeMapEntry<K, V> createEntry(final OTreeMapEntry<K, V> parent);

	public int getNodes() {
		int counter = -1;

		OTreeMapEntry<K, V> entry = getFirstEntry();
		while (entry != null) {
			entry = successor(entry);
			counter++;
		}

		return counter;
	}

	/**
	 * Returns the number of key-value mappings in this map.
	 * 
	 * @return the number of key-value mappings in this map
	 */
	@Override
	public int size() {
		return size;
	}

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
		OTreeMapEntry<K, V> entry = getEntry(key);
		return entry != null;
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
		for (OTreeMapEntry<K, V> e = getFirstEntry(); e != null; e = successor(e))
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
	public V get(Object key) {
		OTreeMapEntry<K, V> entry = getEntry(key);
		return entry == null ? null : entry.getValue();
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
	public void putAll(Map<? extends K, ? extends V> map) {
		int mapSize = map.size();
		if (size == 0 && mapSize != 0 && map instanceof SortedMap) {
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
	 * @return this map's entry for the given key, or <tt>null</tt> if the map does not contain an entry for the key
	 * @throws ClassCastException
	 *           if the specified key cannot be compared with the keys currently in the map
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 */
	final OTreeMapEntry<K, V> getEntry(Object key) {
		return getEntry(key, false);
	}

	final OTreeMapEntry<K, V> getEntry(Object key, boolean iGetContainer) {
		if (key == null)
			return null;

		pageItemFound = false;

		// Offload comparator-based version for sake of performance
		if (comparator != null)
			return getEntryUsingComparator(key, iGetContainer);

		final Comparable<? super K> k = (Comparable<? super K>) key;
		OTreeMapEntry<K, V> p = root;
		OTreeMapEntry<K, V> lastNode = null;

		int beginKey;
		OTreeMapEntry<K, V> left;

		while (p != null) {
			lastNode = p;
			beginKey = k.compareTo(p.getFirstKey());

			if (beginKey == 0) {
				// EXACT MATCH, YOU'RE VERY LUCKY: RETURN THE FIRST KEY WITHOUT SEARCH INSIDE THE NODE
				pageIndex = 0;
				pageItemFound = true;
				return p;
			} else {
				left = p.getLeft();

				if (beginKey < 0 && left != null && k.compareTo(p.getLastKey()) < 0)
					// MINOR THAN THE CURRENT: GET THE LEFT NODE
					p = left;
				else if (beginKey > 0 && p.getRight() != null && k.compareTo(p.getLastKey()) > 0)
					// MAJOR THAN THE CURRENT: GET THE RIGHT NODE
					p = p.getRight();
				else {
					// SEARCH INSIDE THE NODE
					final V value = lastNode.search(k);

					if (value != null)
						// FOUND: RETURN CURRENT NODE
						return lastNode;
					else if (iGetContainer)
						// RETURN THE CONTAINER NODE ANYWAY
						return lastNode;
					break;
				}
			}
		}

		return null;
	}

	public OTreeMapEventListener<K, V> getListener() {
		return listener;
	}

	public void setListener(OTreeMapEventListener<K, V> listener) {
		this.listener = listener;
	}

	/**
	 * Version of getEntry using comparator. Split off from getEntry for performance. (This is not worth doing for most methods, that
	 * are less dependent on comparator performance, but is worthwhile here.)
	 * 
	 * @param iGetContainer
	 */
	final OTreeMapEntry<K, V> getEntryUsingComparator(Object key, boolean iGetContainer) {
		K k = (K) key;
		Comparator<? super K> cpr = comparator;
		if (cpr != null) {
			OTreeMapEntry<K, V> p = root;
			OTreeMapEntry<K, V> lastNode = null;

			while (p != null) {
				lastNode = p;
				int beginKey = cpr.compare(k, p.getFirstKey());
				if (beginKey < 0)
					p = p.getLeft();
				else if (beginKey > 0) {
					if (cpr.compare(k, p.getLastKey()) > 0)
						p = p.getRight();
					else
						break;
				} else {
					pageIndex = 0;
					return p;
				}
			}

			if (lastNode != null) {
				// SEARCH INSIDE THE NODE
				for (int i = 1; i < lastNode.getSize(); ++i) {
					int cmp = cpr.compare(k, lastNode.getKey(i));
					if (cmp == 0) {
						// FOUND: SET THE INDEX AND RETURN THE NODE
						lastNode.getValue(i);
						pageItemFound = true;
						return lastNode;
					} else if (cmp < 0)
						break;
				}

				if (iGetContainer)
					// RETURN THE CONTAINER NODE ANYWAY
					return lastNode;
			}
		}
		return null;
	}

	/**
	 * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry for the least key greater than
	 * the specified key; if no such entry exists (i.e., the greatest key in the Tree is less than the specified key), returns
	 * <tt>null</tt>.
	 */
	final OTreeMapEntry<K, V> getCeilingEntry(K key) {
		OTreeMapEntry<K, V> p = root;
		while (p != null) {
			int cmp = compare(key, p.getKey());
			if (cmp < 0) {
				if (p.getLeft() != null)
					p = p.getLeft();
				else
					return p;
			} else if (cmp > 0) {
				if (p.getRight() != null) {
					p = p.getRight();
				} else {
					OTreeMapEntry<K, V> parent = p.getParent();
					Entry<K, V> ch = p;
					while (parent != null && ch == parent.getRight()) {
						ch = parent;
						parent = parent.getParent();
					}
					return parent;
				}
			} else
				return p;
		}
		return null;
	}

	/**
	 * Gets the entry corresponding to the specified key; if no such entry exists, returns the entry for the greatest key less than
	 * the specified key; if no such entry exists, returns <tt>null</tt>.
	 */
	final OTreeMapEntry<K, V> getFloorEntry(K key) {
		OTreeMapEntry<K, V> p = root;
		while (p != null) {
			int cmp = compare(key, p.getKey());
			if (cmp > 0) {
				if (p.getRight() != null)
					p = p.getRight();
				else
					return p;
			} else if (cmp < 0) {
				if (p.getLeft() != null) {
					p = p.getLeft();
				} else {
					OTreeMapEntry<K, V> parent = p.getParent();
					Entry<K, V> ch = p;
					while (parent != null && ch == parent.getLeft()) {
						ch = parent;
						parent = parent.getParent();
					}
					return parent;
				}
			} else
				return p;

		}
		return null;
	}

	/**
	 * Gets the entry for the least key greater than the specified key; if no such entry exists, returns the entry for the least key
	 * greater than the specified key; if no such entry exists returns <tt>null</tt>.
	 */
	final OTreeMapEntry<K, V> getHigherEntry(K key) {
		OTreeMapEntry<K, V> p = root;
		while (p != null) {
			int cmp = compare(key, p.getKey());
			if (cmp < 0) {
				if (p.getLeft() != null)
					p = p.getLeft();
				else
					return p;
			} else {
				if (p.getRight() != null) {
					p = p.getRight();
				} else {
					OTreeMapEntry<K, V> parent = p.getParent();
					Entry<K, V> ch = p;
					while (parent != null && ch == parent.getRight()) {
						ch = parent;
						parent = parent.getParent();
					}
					return parent;
				}
			}
		}
		return null;
	}

	/**
	 * Returns the entry for the greatest key less than the specified key; if no such entry exists (i.e., the least key in the Tree is
	 * greater than the specified key), returns <tt>null</tt>.
	 */
	final OTreeMapEntry<K, V> getLowerEntry(K key) {
		OTreeMapEntry<K, V> p = root;
		while (p != null) {
			int cmp = compare(key, p.getKey());
			if (cmp > 0) {
				if (p.getRight() != null)
					p = p.getRight();
				else
					return p;
			} else {
				if (p.getLeft() != null) {
					p = p.getLeft();
				} else {
					OTreeMapEntry<K, V> parent = p.getParent();
					Entry<K, V> ch = p;
					while (parent != null && ch == parent.getLeft()) {
						ch = parent;
						parent = parent.getParent();
					}
					return parent;
				}
			}
		}
		return null;
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
		if (root == null) {
			root = createEntry(key, value);

			size = 1;
			modCount++;

			if (listener != null)
				listener.signalTreeChanged(this);

			return null;
		}

		// SEARCH THE ITEM
		OTreeMapEntry<K, V> parent = getEntry(key, true);

		if (pageItemFound)
			// NODE FOUND, UPDATE THE VALUE
			return parent.setValue(value);

		if (parent == null)
			parent = root;

		if (parent.getFreeSpace() > 0) {
			// INSERT INTO THE PAGE
			parent.insert(pageIndex, key, value);
		} else {
			// CREATE NEW NODE AND COPY HALF OF VALUES FROM THE ORIGIN TO THE NEW ONE IN ORDER TO GET VALUES BALANCED
			OTreeMapEntry<K, V> e = createEntry(parent);

			if (pageIndex < parent.getPageSplitItems())
				// INSERT IN THE ORIGINAL NODE
				parent.insert(pageIndex, key, value);
			else
				// INSERT IN THE NEW NODE
				e.insert(pageIndex - parent.getPageSplitItems(), key, value);

			OTreeMapEntry<K, V> prevNode = parent.getRight();
			if (prevNode != null)
				// INSERT THE NODE IN THE TREE IN THE RIGHT MOVING CURRENT RIGHT TO THE RIGHT OF THE NEW NODE
				e.setRight(prevNode);

			parent.setRight(e);

			fixAfterInsertion(e);

			modCount++;
		}

		size++;

		if (listener != null)
			listener.signalTreeChanged(this);

		return null;
	}

	/**
	 * Removes the mapping for this key from this OTreeMap if present.
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
	public V remove(Object key) {
		OTreeMapEntry<K, V> p = getEntry(key);
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
		size = 0;
		root = null;
	}

	/**
	 * Returns a shallow copy of this <tt>OTreeMap</tt> instance. (The keys and values themselves are not cloned.)
	 * 
	 * @return a shallow copy of this map
	 */
	@Override
	public Object clone() {
		OTreeMap<K, V> clone = null;
		try {
			clone = (OTreeMap<K, V>) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError();
		}

		// Put clone into "virgin" state (except for comparator)
		clone.listener = listener;
		clone.lastPageSize = lastPageSize;
		clone.pageIndex = pageIndex;
		clone.pageItemFound = pageItemFound;
		clone.pageLoadFactor = pageLoadFactor;

		clone.root = null;
		clone.size = 0;
		clone.modCount = 0;
		clone.entrySet = null;
		clone.navigableKeySet = null;
		clone.descendingMap = null;

		// Initialize clone with our mappings
		try {
			clone.buildFromSorted(size, entrySet().iterator(), null, null);
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
		OTreeMapEntry<K, V> p = getFirstEntry();
		Map.Entry<K, V> result = exportEntry(p);
		if (p != null)
			deleteEntry(p);
		return result;
	}

	/**
	 * @since 1.6
	 */
	public Entry<K, V> pollLastEntry() {
		OTreeMapEntry<K, V> p = getLastEntry();
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
	public Map.Entry<K, V> lowerEntry(K key) {
		return exportEntry(getLowerEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K lowerKey(K key) {
		return keyOrNull(getLowerEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> floorEntry(K key) {
		return exportEntry(getFloorEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K floorKey(K key) {
		return keyOrNull(getFloorEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> ceilingEntry(K key) {
		return exportEntry(getCeilingEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K ceilingKey(K key) {
		return keyOrNull(getCeilingEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public Map.Entry<K, V> higherEntry(K key) {
		return exportEntry(getHigherEntry(key));
	}

	/**
	 * @throws ClassCastException
	 *           {@inheritDoc}
	 * @throws NullPointerException
	 *           if the specified key is null and this map uses natural ordering, or its comparator does not permit null keys
	 * @since 1.6
	 */
	public K higherKey(K key) {
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
		KeySet<K> nks = navigableKeySet;
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
		Collection<V> vs = values();
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
		EntrySet es = entrySet;
		return (es != null) ? es : (entrySet = new EntrySet());
	}

	/**
	 * @since 1.6
	 */
	public ONavigableMap<K, V> descendingMap() {
		ONavigableMap<K, V> km = descendingMap;
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
	public ONavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
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
	public ONavigableMap<K, V> headMap(K toKey, boolean inclusive) {
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
	public ONavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
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
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
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
	public SortedMap<K, V> headMap(K toKey) {
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
	public SortedMap<K, V> tailMap(K fromKey) {
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
			return OTreeMap.this.size();
		}

		@Override
		public boolean contains(Object o) {
			return OTreeMap.this.containsValue(o);
		}

		@Override
		public boolean remove(Object o) {
			for (OTreeMapEntry<K, V> e = getFirstEntry(); e != null; e = successor(e)) {
				if (valEquals(e.getValue(), o)) {
					deleteEntry(e);
					return true;
				}
			}
			return false;
		}

		@Override
		public void clear() {
			OTreeMap.this.clear();
		}
	}

	class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator(getFirstEntry());
		}

		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			OTreeMapEntry<K, V> entry = (OTreeMapEntry<K, V>) o;
			V value = entry.getValue();
			V p = get(entry.getKey());
			return p != null && valEquals(p, value);
		}

		@Override
		public boolean remove(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			OTreeMapEntry<K, V> entry = (OTreeMapEntry<K, V>) o;
			V value = entry.getValue();
			OTreeMapEntry<K, V> p = getEntry(entry.getKey());
			if (p != null && valEquals(p.getValue(), value)) {
				deleteEntry(p);
				return true;
			}
			return false;
		}

		@Override
		public int size() {
			return OTreeMap.this.size();
		}

		@Override
		public void clear() {
			OTreeMap.this.clear();
		}
	}

	/*
	 * Unlike Values and EntrySet, the KeySet class is static, delegating to a ONavigableMap to allow use by SubMaps, which outweighs
	 * the ugliness of needing type-tests for the following Iterator methods that are defined appropriately in main versus submap
	 * classes.
	 */

	Iterator<K> keyIterator() {
		return new KeyIterator(getFirstEntry());
	}

	Iterator<K> descendingKeyIterator() {
		return new DescendingKeyIterator(getLastEntry());
	}

	@SuppressWarnings("rawtypes")
	static final class KeySet<E> extends AbstractSet<E> implements ONavigableSet<E> {
		private final ONavigableMap<E, Object>	m;

		KeySet(ONavigableMap<E, Object> map) {
			m = map;
		}

		@Override
		public Iterator<E> iterator() {
			if (m instanceof OTreeMap)
				return ((OTreeMap<E, Object>) m).keyIterator();
			else
				return (((OTreeMap.NavigableSubMap) m).keyIterator());
		}

		public Iterator<E> descendingIterator() {
			if (m instanceof OTreeMap)
				return ((OTreeMap<E, Object>) m).descendingKeyIterator();
			else
				return (((OTreeMap.NavigableSubMap) m).descendingKeyIterator());
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
		public boolean contains(Object o) {
			return m.containsKey(o);
		}

		@Override
		public void clear() {
			m.clear();
		}

		public E lower(E e) {
			return m.lowerKey(e);
		}

		public E floor(E e) {
			return m.floorKey(e);
		}

		public E ceiling(E e) {
			return m.ceilingKey(e);
		}

		public E higher(E e) {
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
			Map.Entry<E, Object> e = m.pollFirstEntry();
			return e == null ? null : e.getKey();
		}

		public E pollLast() {
			Map.Entry<E, Object> e = m.pollLastEntry();
			return e == null ? null : e.getKey();
		}

		@Override
		public boolean remove(Object o) {
			int oldSize = size();
			m.remove(o);
			return size() != oldSize;
		}

		public ONavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
			return new OTreeSetMemory<E>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
		}

		public ONavigableSet<E> headSet(E toElement, boolean inclusive) {
			return new OTreeSetMemory<E>(m.headMap(toElement, inclusive));
		}

		public ONavigableSet<E> tailSet(E fromElement, boolean inclusive) {
			return new OTreeSetMemory<E>(m.tailMap(fromElement, inclusive));
		}

		public SortedSet<E> subSet(E fromElement, E toElement) {
			return subSet(fromElement, true, toElement, false);
		}

		public SortedSet<E> headSet(E toElement) {
			return headSet(toElement, false);
		}

		public SortedSet<E> tailSet(E fromElement) {
			return tailSet(fromElement, true);
		}

		public ONavigableSet<E> descendingSet() {
			return new OTreeSetMemory<E>(m.descendingMap());
		}
	}

	final class EntryIterator extends AbstractEntryIterator<K, V, Map.Entry<K, V>> {
		EntryIterator(OTreeMapEntry<K, V> first) {
			super(first);
		}

		public Map.Entry<K, V> next() {
			return nextEntry();
		}
	}

	final class ValueIterator extends AbstractEntryIterator<K, V, V> {
		ValueIterator(OTreeMapEntry<K, V> first) {
			super(first);
		}

		public V next() {
			return nextEntry().getValue();
		}
	}

	final class KeyIterator extends AbstractEntryIterator<K, V, K> {
		KeyIterator(OTreeMapEntry<K, V> first) {
			super(first);
		}

		public K next() {
			return nextEntry().getKey();
		}
	}

	final class DescendingKeyIterator extends AbstractEntryIterator<K, V, K> {
		DescendingKeyIterator(OTreeMapEntry<K, V> first) {
			super(first);
		}

		public K next() {
			return prevEntry().getKey();
		}
	}

	// Little utilities

	/**
	 * Compares two keys using the correct comparison method for this OTreeMap.
	 */
	final int compare(Object k1, Object k2) {
		return comparator == null ? ((Comparable<? super K>) k1).compareTo((K) k2) : comparator.compare((K) k1, (K) k2);
	}

	/**
	 * Test two values for equality. Differs from o1.equals(o2) only in that it copes with <tt>null</tt> o1 properly.
	 */
	final static boolean valEquals(Object o1, Object o2) {
		return (o1 == null ? o2 == null : o1.equals(o2));
	}

	/**
	 * Return SimpleImmutableEntry for entry, or null if null
	 */
	static <K, V> Map.Entry<K, V> exportEntry(OTreeMapEntry<K, V> e) {
		return e == null ? null : new AbstractMap.SimpleImmutableEntry<K, V>(e);
	}

	/**
	 * Return key for entry, or null if null
	 */
	static <K, V> K keyOrNull(OTreeMapEntry<K, V> e) {
		return e == null ? null : e.getKey();
	}

	/**
	 * Returns the key corresponding to the specified Entry.
	 * 
	 * @throws NoSuchElementException
	 *           if the Entry is null
	 */
	static <K> K key(OTreeMapEntry<K, ?> e) {
		if (e == null)
			throw new NoSuchElementException();
		return e.getKey();
	}

	// SubMaps

	/**
	 * @serial include
	 */
	@SuppressWarnings("serial")
	static abstract class NavigableSubMap<K, V> extends AbstractMap<K, V> implements ONavigableMap<K, V>, java.io.Serializable {
		/**
		 * The backing map.
		 */
		final OTreeMap<K, V>	m;

		/**
		 * Endpoints are represented as triples (fromStart, lo, loInclusive) and (toEnd, hi, hiInclusive). If fromStart is true, then
		 * the low (absolute) bound is the start of the backing map, and the other values are ignored. Otherwise, if loInclusive is
		 * true, lo is the inclusive bound, else lo is the exclusive bound. Similarly for the upper bound.
		 */
		final K								lo, hi;
		final boolean					fromStart, toEnd;
		final boolean					loInclusive, hiInclusive;

		NavigableSubMap(OTreeMap<K, V> m, boolean fromStart, K lo, boolean loInclusive, boolean toEnd, K hi, boolean hiInclusive) {
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

		final boolean tooLow(Object key) {
			if (!fromStart) {
				int c = m.compare(key, lo);
				if (c < 0 || (c == 0 && !loInclusive))
					return true;
			}
			return false;
		}

		final boolean tooHigh(Object key) {
			if (!toEnd) {
				int c = m.compare(key, hi);
				if (c > 0 || (c == 0 && !hiInclusive))
					return true;
			}
			return false;
		}

		final boolean inRange(Object key) {
			return !tooLow(key) && !tooHigh(key);
		}

		final boolean inClosedRange(Object key) {
			return (fromStart || m.compare(key, lo) >= 0) && (toEnd || m.compare(hi, key) >= 0);
		}

		final boolean inRange(Object key, boolean inclusive) {
			return inclusive ? inRange(key) : inClosedRange(key);
		}

		/*
		 * Absolute versions of relation operations. Subclasses map to these using like-named "sub" versions that invert senses for
		 * descending maps
		 */

		final OTreeMapEntry<K, V> absLowest() {
			OTreeMapEntry<K, V> e = (fromStart ? m.getFirstEntry() : (loInclusive ? m.getCeilingEntry(lo) : m.getHigherEntry(lo)));
			return (e == null || tooHigh(e.getKey())) ? null : e;
		}

		final OTreeMapEntry<K, V> absHighest() {
			OTreeMapEntry<K, V> e = (toEnd ? m.getLastEntry() : (hiInclusive ? m.getFloorEntry(hi) : m.getLowerEntry(hi)));
			return (e == null || tooLow(e.getKey())) ? null : e;
		}

		final OTreeMapEntry<K, V> absCeiling(K key) {
			if (tooLow(key))
				return absLowest();
			OTreeMapEntry<K, V> e = m.getCeilingEntry(key);
			return (e == null || tooHigh(e.getKey())) ? null : e;
		}

		final OTreeMapEntry<K, V> absHigher(K key) {
			if (tooLow(key))
				return absLowest();
			OTreeMapEntry<K, V> e = m.getHigherEntry(key);
			return (e == null || tooHigh(e.getKey())) ? null : e;
		}

		final OTreeMapEntry<K, V> absFloor(K key) {
			if (tooHigh(key))
				return absHighest();
			OTreeMapEntry<K, V> e = m.getFloorEntry(key);
			return (e == null || tooLow(e.getKey())) ? null : e;
		}

		final OTreeMapEntry<K, V> absLower(K key) {
			if (tooHigh(key))
				return absHighest();
			OTreeMapEntry<K, V> e = m.getLowerEntry(key);
			return (e == null || tooLow(e.getKey())) ? null : e;
		}

		/** Returns the absolute high fence for ascending traversal */
		final OTreeMapEntry<K, V> absHighFence() {
			return (toEnd ? null : (hiInclusive ? m.getHigherEntry(hi) : m.getCeilingEntry(hi)));
		}

		/** Return the absolute low fence for descending traversal */
		final OTreeMapEntry<K, V> absLowFence() {
			return (fromStart ? null : (loInclusive ? m.getLowerEntry(lo) : m.getFloorEntry(lo)));
		}

		// Abstract methods defined in ascending vs descending classes
		// These relay to the appropriate absolute versions

		abstract OTreeMapEntry<K, V> subLowest();

		abstract OTreeMapEntry<K, V> subHighest();

		abstract OTreeMapEntry<K, V> subCeiling(K key);

		abstract OTreeMapEntry<K, V> subHigher(K key);

		abstract OTreeMapEntry<K, V> subFloor(K key);

		abstract OTreeMapEntry<K, V> subLower(K key);

		/** Returns ascending iterator from the perspective of this submap */
		abstract Iterator<K> keyIterator();

		/** Returns descending iterator from the perspective of this submap */
		abstract Iterator<K> descendingKeyIterator();

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
			OTreeMapEntry<K, V> e = subLowest();
			Map.Entry<K, V> result = exportEntry(e);
			if (e != null)
				m.deleteEntry(e);
			return result;
		}

		public final Map.Entry<K, V> pollLastEntry() {
			OTreeMapEntry<K, V> e = subHighest();
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
			return (nksv != null) ? nksv : (navigableKeySetView = new OTreeMap.KeySet(this));
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
				OTreeMapEntry<K, V> n = absLowest();
				return n == null || tooHigh(n.getKey());
			}

			@Override
			public boolean contains(final Object o) {
				if (!(o instanceof OTreeMapEntry))
					return false;
				OTreeMapEntry<K, V> entry = (OTreeMapEntry<K, V>) o;
				K key = entry.getKey();
				if (!inRange(key))
					return false;
				V nodeValue = m.get(key);
				return nodeValue != null && valEquals(nodeValue, entry.getValue());
			}

			@Override
			public boolean remove(final Object o) {
				if (!(o instanceof OTreeMapEntry))
					return false;
				final OTreeMapEntry<K, V> entry = (OTreeMapEntry<K, V>) o;
				K key = entry.getKey();
				if (!inRange(key))
					return false;
				final OTreeMapEntry<K, V> node = m.getEntry(key);
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
		abstract class SubMapIterator<T> implements Iterator<T> {
			OTreeMapEntry<K, V>	lastReturned;
			OTreeMapEntry<K, V>	next;
			final K							fenceKey;
			int									expectedModCount;

			SubMapIterator(final OTreeMapEntry<K, V> first, final OTreeMapEntry<K, V> fence) {
				expectedModCount = m.modCount;
				lastReturned = null;
				next = first;
				fenceKey = fence == null ? null : fence.getKey();
			}

			public final boolean hasNext() {
				return next != null && next.getKey() != fenceKey;
			}

			final OTreeMapEntry<K, V> nextEntry() {
				OTreeMapEntry<K, V> e = next;
				if (e == null || e.getKey() == fenceKey)
					throw new NoSuchElementException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				next = successor(e);
				lastReturned = e;
				return e;
			}

			final OTreeMapEntry<K, V> prevEntry() {
				OTreeMapEntry<K, V> e = next;
				if (e == null || e.getKey() == fenceKey)
					throw new NoSuchElementException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				next = predecessor(e);
				lastReturned = e;
				return e;
			}

			final void removeAscending() {
				if (lastReturned == null)
					throw new IllegalStateException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				// deleted entries are replaced by their successors
				if (lastReturned.getLeft() != null && lastReturned.getRight() != null)
					next = lastReturned;
				m.deleteEntry(lastReturned);
				lastReturned = null;
				expectedModCount = m.modCount;
			}

			final void removeDescending() {
				if (lastReturned == null)
					throw new IllegalStateException();
				if (m.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				m.deleteEntry(lastReturned);
				lastReturned = null;
				expectedModCount = m.modCount;
			}

		}

		final class SubMapEntryIterator extends SubMapIterator<Map.Entry<K, V>> {
			SubMapEntryIterator(final OTreeMapEntry<K, V> first, final OTreeMapEntry<K, V> fence) {
				super(first, fence);
			}

			public Map.Entry<K, V> next() {
				return nextEntry();
			}

			public void remove() {
				removeAscending();
			}
		}

		final class SubMapKeyIterator extends SubMapIterator<K> {
			SubMapKeyIterator(final OTreeMapEntry<K, V> first, final OTreeMapEntry<K, V> fence) {
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
			DescendingSubMapEntryIterator(final OTreeMapEntry<K, V> last, final OTreeMapEntry<K, V> fence) {
				super(last, fence);
			}

			public Map.Entry<K, V> next() {
				return prevEntry();
			}

			public void remove() {
				removeDescending();
			}
		}

		final class DescendingSubMapKeyIterator extends SubMapIterator<K> {
			DescendingSubMapKeyIterator(final OTreeMapEntry<K, V> last, final OTreeMapEntry<K, V> fence) {
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

		AscendingSubMap(final OTreeMap<K, V> m, final boolean fromStart, final K lo, final boolean loInclusive, final boolean toEnd,
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
		Iterator<K> keyIterator() {
			return new SubMapKeyIterator(absLowest(), absHighFence());
		}

		@Override
		Iterator<K> descendingKeyIterator() {
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
		OTreeMapEntry<K, V> subLowest() {
			return absLowest();
		}

		@Override
		OTreeMapEntry<K, V> subHighest() {
			return absHighest();
		}

		@Override
		OTreeMapEntry<K, V> subCeiling(final K key) {
			return absCeiling(key);
		}

		@Override
		OTreeMapEntry<K, V> subHigher(final K key) {
			return absHigher(key);
		}

		@Override
		OTreeMapEntry<K, V> subFloor(final K key) {
			return absFloor(key);
		}

		@Override
		OTreeMapEntry<K, V> subLower(final K key) {
			return absLower(key);
		}
	}

	/**
	 * @serial include
	 */
	static final class DescendingSubMap<K, V> extends NavigableSubMap<K, V> {
		private static final long						serialVersionUID	= 912986545866120460L;

		private final Comparator<? super K>	reverseComparator	= Collections.reverseOrder(m.comparator);

		DescendingSubMap(final OTreeMap<K, V> m, final boolean fromStart, final K lo, final boolean loInclusive, final boolean toEnd,
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
		Iterator<K> keyIterator() {
			return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
		}

		@Override
		Iterator<K> descendingKeyIterator() {
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
		OTreeMapEntry<K, V> subLowest() {
			return absHighest();
		}

		@Override
		OTreeMapEntry<K, V> subHighest() {
			return absLowest();
		}

		@Override
		OTreeMapEntry<K, V> subCeiling(final K key) {
			return absFloor(key);
		}

		@Override
		OTreeMapEntry<K, V> subHigher(final K key) {
			return absLower(key);
		}

		@Override
		OTreeMapEntry<K, V> subFloor(final K key) {
			return absCeiling(key);
		}

		@Override
		OTreeMapEntry<K, V> subLower(final K key) {
			return absHigher(key);
		}
	}

	// Red-black mechanics

	private static final boolean	RED		= false;
	static final boolean					BLACK	= true;

	/**
	 * Node in the Tree. Doubles as a means to pass key-value pairs back to user (see Map.Entry).
	 */

	/**
	 * Returns the first Entry in the OTreeMap (according to the OTreeMap's key-sort function). Returns null if the OTreeMap is empty.
	 */
	final OTreeMapEntry<K, V> getFirstEntry() {
		OTreeMapEntry<K, V> p = root;
		if (p != null) {
			if (p.getSize() > 0)
				pageIndex = 0;

			while (p.getLeft() != null)
				p = p.getLeft();
		}
		return p;
	}

	/**
	 * Returns the last Entry in the OTreeMap (according to the OTreeMap's key-sort function). Returns null if the OTreeMap is empty.
	 */
	final OTreeMapEntry<K, V> getLastEntry() {
		OTreeMapEntry<K, V> p = root;
		if (p != null)
			while (p.getRight() != null)
				p = p.getRight();

		if (p != null)
			pageIndex = p.getSize() - 1;

		return p;
	}

	/**
	 * Returns the successor of the specified Entry, or null if no such.
	 */
	static <K, V> OTreeMapEntry<K, V> successor(OTreeMapEntry<K, V> t) {
		if (t == null)
			return null;

		OTreeMapEntry<K, V> p = null;
		if (t.getRight() != null) {
			p = t.getRight();
			while (p.getLeft() != null)
				p = p.getLeft();
		} else {
			p = t.getParent();
			while (p != null && t == p.getRight()) {
				t = p;
				p = p.getParent();
			}
		}

		return p;
	}

	/**
	 * Returns the predecessor of the specified Entry, or null if no such.
	 */
	static <K, V> OTreeMapEntry<K, V> predecessor(final OTreeMapEntry<K, V> t) {
		if (t == null)
			return null;
		else if (t.getLeft() != null) {
			OTreeMapEntry<K, V> p = t.getLeft();
			while (p.getRight() != null)
				p = p.getRight();
			return p;
		} else {
			OTreeMapEntry<K, V> p = t.getParent();
			Entry<K, V> ch = t;
			while (p != null && ch == p.getLeft()) {
				ch = p;
				p = p.getParent();
			}
			return p;
		}
	}

	/**
	 * Balancing operations.
	 * 
	 * Implementations of rebalancings during insertion and deletion are slightly different than the CLR version. Rather than using
	 * dummy nilnodes, we use a set of accessors that deal properly with null. They are used to avoid messiness surrounding nullness
	 * checks in the main algorithms.
	 */

	private static <K, V> boolean colorOf(final OTreeMapEntry<K, V> p) {
		return (p == null ? BLACK : p.getColor());
	}

	private static <K, V> OTreeMapEntry<K, V> parentOf(final OTreeMapEntry<K, V> p) {
		return (p == null ? null : p.getParent());
	}

	private static <K, V> void setColor(final OTreeMapEntry<K, V> p, final boolean c) {
		if (p != null)
			p.setColor(c);
	}

	private static <K, V> OTreeMapEntry<K, V> leftOf(final OTreeMapEntry<K, V> p) {
		return (p == null) ? null : p.getLeft();
	}

	private static <K, V> OTreeMapEntry<K, V> rightOf(final OTreeMapEntry<K, V> p) {
		return (p == null) ? null : p.getRight();
	}

	/** From CLR */
	private void rotateLeft(final OTreeMapEntry<K, V> p) {
		if (p != null) {
			OTreeMapEntry<K, V> r = p.getRight();
			p.setRight(r.getLeft());
			if (r.getLeft() != null)
				r.getLeft().setParent(p);
			r.setParent(p.getParent());
			if (p.getParent() == null)
				root = r;
			else if (p.getParent().getLeft() == p)
				p.getParent().setLeft(r);
			else
				p.getParent().setRight(r);
			r.setLeft(p);
			p.setParent(r);
		}
	}

	/** From CLR */
	private void rotateRight(final OTreeMapEntry<K, V> p) {
		if (p != null) {
			OTreeMapEntry<K, V> l = p.getLeft();
			p.setLeft(l.getRight());
			if (l.getRight() != null)
				l.getRight().setParent(p);
			l.setParent(p.getParent());
			if (p.getParent() == null)
				root = l;
			else if (p.getParent().getRight() == p)
				p.getParent().setRight(l);
			else
				p.getParent().setLeft(l);
			l.setRight(p);
			p.setParent(l);
		}
	}

	/** From CLR */
	private void fixAfterInsertion(OTreeMapEntry<K, V> x) {
		x.setColor(RED);

		while (x != null && x != root && x.getParent().getColor() == RED) {
			if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
				OTreeMapEntry<K, V> y = rightOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == rightOf(parentOf(x))) {
						x = parentOf(x);
						rotateLeft(x);
					}
					setColor(parentOf(x), BLACK);
					setColor(parentOf(parentOf(x)), RED);
					rotateRight(parentOf(parentOf(x)));
				}
			} else {
				OTreeMapEntry<K, V> y = leftOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == leftOf(parentOf(x))) {
						x = parentOf(x);
						rotateRight(x);
					}
					setColor(parentOf(x), BLACK);
					setColor(parentOf(parentOf(x)), RED);
					rotateLeft(parentOf(parentOf(x)));
				}
			}
		}
		root.setColor(BLACK);
	}

	/**
	 * Delete node p, and then rebalance the tree.
	 * 
	 * @param iIndex
	 *          -1 = delete the node, otherwise the item inside of it
	 */
	void deleteEntry(OTreeMapEntry<K, V> p) {
		size--;

		if (listener != null)
			listener.signalTreeChanged(this);

		if (pageIndex > -1) {
			// DELETE INSIDE THE NODE
			p.remove();

			if (p.getSize() > 0)
				return;
		}

		// DELETE THE ENTIRE NODE, RE-BUILDING THE STRUCTURE

		modCount++;
		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p.getLeft() != null && p.getRight() != null) {
			OTreeMapEntry<K, V> s = successor(p);
			p.copyFrom(s);
			p = s;
		} // p has 2 children

		// Start fixup at replacement node, if it exists.
		final OTreeMapEntry<K, V> replacement = (p.getLeft() != null ? p.getLeft() : p.getRight());

		if (replacement != null) {
			// Link replacement to parent
			replacement.setParent(p.getParent());
			if (p.getParent() == null)
				root = replacement;
			else if (p == p.getParent().getLeft())
				p.getParent().setLeft(replacement);
			else
				p.getParent().setRight(replacement);

			// Null out links so they are OK to use by fixAfterDeletion.
			p.setLeft(p.setRight(p.setParent(null)));

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
	private void fixAfterDeletion(OTreeMapEntry<K, V> x) {
		while (x != root && colorOf(x) == BLACK) {
			if (x == leftOf(parentOf(x))) {
				OTreeMapEntry<K, V> sib = rightOf(parentOf(x));

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
				OTreeMapEntry<K, V> sib = leftOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateRight(parentOf(x));
					sib = leftOf(parentOf(x));
				}

				if (colorOf(rightOf(sib)) == BLACK && colorOf(leftOf(sib)) == BLACK) {
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

	private static final long	serialVersionUID	= 919286545866124006L;

	/**
	 * Save the state of the <tt>OTreeMap</tt> instance to a stream (i.e., serialize it).
	 * 
	 * @serialData The <i>size</i> of the OTreeMap (the number of key-value mappings) is emitted (int), followed by the key (Object)
	 *             and value (Object) for each key-value mapping represented by the OTreeMap. The key-value mappings are emitted in
	 *             key-order (as determined by the OTreeMap's Comparator, or by the keys' natural ordering if the OTreeMap has no
	 *             Comparator).
	 */
	private void writeObject(final java.io.ObjectOutputStream s) throws java.io.IOException {
		// Write out the Comparator and any hidden stuff
		s.defaultWriteObject();

		// Write out size (number of Mappings)
		s.writeInt(size);

		// Write out keys and values (alternating)
		for (Iterator<Map.Entry<K, V>> i = entrySet().iterator(); i.hasNext();) {
			Entry<K, V> e = i.next();
			s.writeObject(e.getKey());
			s.writeObject(e.getValue());
		}
	}

	/**
	 * Reconstitute the <tt>OTreeMap</tt> instance from a stream (i.e., deserialize it).
	 */
	private void readObject(final java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		// Read in the Comparator and any hidden stuff
		s.defaultReadObject();

		// Read in size
		int size = s.readInt();

		buildFromSorted(size, null, s, null);
	}

	/** Intended to be called only from OTreeSet.readObject */
	void readOTreeSet(int size, java.io.ObjectInputStream s, V defaultVal) throws java.io.IOException, ClassNotFoundException {
		buildFromSorted(size, null, s, defaultVal);
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
	 * It is assumed that the comparator of the OTreeMap is already set prior to calling this method.
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
		this.size = size;
		root = buildFromSorted(0, 0, size - 1, computeRedLevel(size), it, str, defaultVal);
	}

	/**
	 * Recursive "helper method" that does the real work of the previous method. Identically named parameters have identical
	 * definitions. Additional parameters are documented below. It is assumed that the comparator and size fields of the OTreeMap are
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
	private final OTreeMapEntry<K, V> buildFromSorted(final int level, final int lo, final int hi, final int redLevel,
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

		OTreeMapEntry<K, V> left = null;
		if (lo < mid)
			left = buildFromSorted(level + 1, lo, mid - 1, redLevel, it, str, defaultVal);

		// extract key and/or value from iterator or stream
		K key;
		V value;
		if (it != null) {
			if (defaultVal == null) {
				OTreeMapEntry<K, V> entry = (OTreeMapEntry<K, V>) it.next();
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

		final OTreeMapEntry<K, V> middle = createEntry(key, value);

		// color nodes in non-full bottom most level red
		if (level == redLevel)
			middle.setColor(RED);

		if (left != null) {
			middle.setLeft(left);
			left.setParent(middle);
		}

		if (mid < hi) {
			OTreeMapEntry<K, V> right = buildFromSorted(level + 1, mid + 1, hi, redLevel, it, str, defaultVal);
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

	public int getPageSize() {
		return lastPageSize;
	}

	public int getPageIndex() {
		return pageIndex;
	}

	private void init() {
	}
}
