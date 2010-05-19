package com.orientechnologies.common.collection;

import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class OTreeMapEntry<K, V> implements Map.Entry<K, V> {
	protected OTreeMap<K, V>	tree;

	protected int							size										= 1;
	protected int							pageSize;

	protected K[]							keys;
	protected V[]							values;
	protected boolean					color										= OTreeMap.BLACK;

	private int								pageSplitItems;
	private static final int	BINARY_SEARCH_THRESHOLD	= 10;

	/**
	 * Constructor called on unmarshalling.
	 * 
	 */
	protected OTreeMapEntry(final OTreeMap<K, V> iTree) {
		this.tree = iTree;
		init();
	}

	/**
	 * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and BLACK color.
	 */
	protected OTreeMapEntry(final OTreeMap<K, V> iTree, final K iKey, final V iValue, final OTreeMapEntry<K, V> iParent) {
		this.tree = iTree;
		setParent(iParent);
		this.pageSize = tree.getPageSize();
		this.keys = (K[]) new Object[pageSize];
		this.keys[0] = iKey;
		this.values = (V[]) new Object[pageSize];
		this.values[0] = iValue;
		init();
	}

	/**
	 * Copy values from the parent node.
	 * 
	 * @param iParent
	 * @param iPosition
	 */
	protected OTreeMapEntry(final OTreeMapEntry<K, V> iParent, final int iPosition) {
		this.tree = iParent.tree;
		setParent(iParent);
		this.pageSize = tree.getPageSize();
		this.keys = (K[]) new Object[pageSize];
		this.size = iParent.size - iPosition;
		System.arraycopy(iParent.keys, iPosition, keys, 0, size);
		this.values = (V[]) new Object[pageSize];
		System.arraycopy(iParent.values, iPosition, values, 0, size);

		iParent.size = iPosition;
		init();
	}

	public abstract void setLeft(OTreeMapEntry<K, V> left);

	public abstract OTreeMapEntry<K, V> getLeft();

	public abstract OTreeMapEntry<K, V> setRight(OTreeMapEntry<K, V> right);

	public abstract OTreeMapEntry<K, V> getRight();

	public abstract OTreeMapEntry<K, V> setParent(OTreeMapEntry<K, V> parent);

	public abstract OTreeMapEntry<K, V> getParent();

	protected OTreeMap<K, V> getTree() {
		return tree;
	}

	/**
	 * Returns the key.
	 * 
	 * @return the key
	 */
	public K getKey() {
		return getKey(tree.pageIndex);
	}

	public K getKey(final int iIndex) {
		if (iIndex >= size)
			throw new IndexOutOfBoundsException("Requested index " + iIndex + " when the range is 0-" + size);

		tree.pageIndex = iIndex;
		return getKeyAt(iIndex);
	}

	protected K getKeyAt(final int iIndex) {
		return keys[iIndex];
	}

	/**
	 * Returns the value associated with the key.
	 * 
	 * @return the value associated with the key
	 */
	public V getValue() {
		if (tree.pageIndex == -1)
			return getValueAt(0);

		return getValueAt(tree.pageIndex);
	}

	public V getValue(final int iIndex) {
		tree.pageIndex = iIndex;
		return getValueAt(iIndex);
	}

	protected V getValueAt(int iIndex) {
		return values[iIndex];
	}

	/**
	 * Replaces the value currently associated with the key with the given value.
	 * 
	 * @return the value associated with the key before this method was called
	 */
	public V setValue(final V value) {
		V oldValue = this.getValue();
		this.values[tree.pageIndex] = value;
		return oldValue;
	}

	public int getFreeSpace() {
		return pageSize - size;
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof OTreeMapEntry<?, ?>))
			return false;
		final OTreeMapEntry<?, ?> e = (OTreeMapEntry<?, ?>) o;

		return OTreeMap.valEquals(getKey(0), e.getKey(0)) && OTreeMap.valEquals(getValue(0), e.getValue(0));
	}

	@Override
	public int hashCode() {
		int keyHash = (keys == null ? 0 : keys.hashCode());
		int valueHash = (values == null ? 0 : values.hashCode());
		return keyHash ^ valueHash;
	}

	@Override
	public String toString() {
		final StringBuilder buffer = new StringBuilder("[");
		for (int i = 0; i < size; ++i) {
			if (i > 0)
				buffer.append(",");

			buffer.append(keys[i] != null ? keys[i] : "{lazy}");
		}

		buffer.append("]");

		return buffer.toString();
	}

	/**
	 * Execute a binary search between the keys of the node. The keys are always kept ordered. It update the pageIndex attribute with
	 * the most closer key found (useful for the next inserting).
	 * 
	 * @param iKey
	 *          Key to find
	 * @return The value found if any, otherwise null
	 */
	protected V search(final Comparable<? super K> iKey) {
		tree.pageItemFound = false;

		if (size == 0)
			return null;

		// if (((Comparable) getKeyAt(size - 1)).compareTo(iKey) < 0) {
		// CHECK THE LOWER LIMIT
		int comp = iKey.compareTo(getKeyAt(0));
		if (comp == 0) {
			// FOUND: SET THE INDEX AND RETURN THE NODE
			tree.pageItemFound = true;
			tree.pageIndex = 0;
			return getValueAt(tree.pageIndex);

		} else if (comp < 0) {
			// KEY OUT OF FIRST ITEM: AVOID SEARCH AND RETURN THE FIRST POSITION
			tree.pageIndex = 0;
			return null;

		} else {
			// CHECK THE UPPER LIMIT
			comp = iKey.compareTo(getKeyAt(size - 1));

			if (comp > 0) {
				// KEY OUT OF LAST ITEM: AVOID SEARCH AND RETURN THE LAST POSITION
				tree.pageIndex = size;
				return null;
			}
		}

		if (size < BINARY_SEARCH_THRESHOLD)
			return linearSearch(iKey);
		else
			return binarySearch(iKey);
	}

	/**
	 * Linear search inside the node
	 * 
	 * @param iKey
	 *          Key to search
	 * @return Value if found, otherwise null and the tree.pageIndex updated with the closest-after-first position valid for further
	 *         inserts.
	 */
	private V linearSearch(final Comparable<? super K> iKey) {
		V value = null;
		int i = 0;
		int comp = -1;
		for (; i < size; ++i) {
			comp = ((Comparable) getKeyAt(i)).compareTo(iKey);

			if (comp == 0) {
				// FOUND: SET THE INDEX AND RETURN THE NODE
				tree.pageItemFound = true;
				value = getValueAt(tree.pageIndex);
				break;
			} else if (comp > 0)
				break;
		}

		tree.pageIndex = i;

		return value;
	}

	/**
	 * Binary search inside the node
	 * 
	 * @param iKey
	 *          Key to search
	 * @return Value if found, otherwise null and the tree.pageIndex updated with the closest-after-first position valid for further
	 *         inserts.
	 */
	private V binarySearch(final Comparable<? super K> iKey) {
		int low = 0;
		int high = size - 1;
		int mid = 0;

		while (low <= high) {
			mid = (low + high) >>> 1;
			Comparable midVal = (Comparable) getKeyAt(mid);
			int cmp = midVal.compareTo(iKey);

			if (cmp == 0) {
				// FOUND: SET THE INDEX AND RETURN THE NODE
				tree.pageItemFound = true;
				tree.pageIndex = mid;
				return getValueAt(tree.pageIndex);
			}

			if (low == high)
				break;

			if (cmp < 0)
				low = mid + 1;
			else
				high = mid;
		}

		tree.pageIndex = mid;
		return null;
	}

	protected void insert(final int iPosition, final K key, final V value) {
		if (iPosition < size) {
			// MOVE RIGHT TO MAKE ROOM FOR THE ITEM
			System.arraycopy(keys, iPosition, keys, iPosition + 1, size - iPosition);
			System.arraycopy(values, iPosition, values, iPosition + 1, size - iPosition);
		}

		keys[iPosition] = key;
		values[iPosition] = value;
		size++;
	}

	protected void remove() {
		if (tree.pageIndex == size - 1) {
			// LAST ONE: JUST REMOVE IT
		} else if (tree.pageIndex > -1) {
			// SHIFT LEFT THE VALUES
			System.arraycopy(keys, tree.pageIndex + 1, keys, tree.pageIndex, size - tree.pageIndex - 1);
			System.arraycopy(values, tree.pageIndex + 1, values, tree.pageIndex, size - tree.pageIndex - 1);
		}

		// FREE RESOURCES
		keys[size - 1] = null;
		values[size - 1] = null;

		size--;
		tree.pageIndex = -1;
	}

	protected void setColor(boolean color) {
		this.color = color;
	}

	protected boolean getColor() {
		return color;
	}

	public int getSize() {
		return size;
	}

	public K getLastKey() {
		return getKey(size - 1);
	}

	public K getFirstKey() {
		return getKey(0);
	}

	protected void copyFrom(final OTreeMapEntry<K, V> iSource) {
		keys = iSource.keys;
		values = iSource.values;
		size = iSource.size;
	}

	public int getPageSplitItems() {
		return pageSplitItems;
	}

	protected void init() {
		pageSplitItems = (int) (pageSize * tree.pageLoadFactor);
	}
}