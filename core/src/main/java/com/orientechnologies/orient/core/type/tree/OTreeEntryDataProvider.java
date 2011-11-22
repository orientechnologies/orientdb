package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORID;

public interface OTreeEntryDataProvider<K, V> {

	public ORID getIdentity();

	public K getKeyAt(int iIndex);

	public V getValueAt(int iIndex);

	public ORID getParent();

	public ORID getLeft();

	public ORID getRight();

	public int getSize();

	public int getPageSize();

	public boolean getColor();

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean setValueAt(int iIndex, V iValue);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean insertAt(int iIndex, K iKey, V iValue);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean removeAt(int iIndex);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean copyDataFrom(OTreeEntryDataProvider<K, V> iFrom, int iStartPosition);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean truncate(int iNewSize);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean setParent(ORID iRid);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean setLeft(ORID iRid);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean setRight(ORID iRid);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean setColor(final boolean iColor);

	/**
	 * @return <code>true</code> if this entry become dirty with this update.
	 */
	public boolean copyFrom(OTreeEntryDataProvider<K, V> iSource);

	public boolean isEntryDirty();

	public void save();

	public void delete();

	/** SPEED UP MEMORY CLAIM BY RESETTING INTERNAL FIELDS */
	public void clear();

}
