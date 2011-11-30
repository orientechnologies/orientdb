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
package com.orientechnologies.orient.core.type.tree.provider;

import com.orientechnologies.orient.core.id.ORID;

/**
 * Interface to handle persistence of a tree node.
 * 
 * @author Sylvain Spinelli (sylvain.spinelli@kelis.fr)
 * 
 * @param <K>
 *          Key
 * @param <V>
 *          Value
 */
public interface OMVRBTreeEntryDataProvider<K, V> {

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
	public boolean copyDataFrom(OMVRBTreeEntryDataProvider<K, V> iFrom, int iStartPosition);

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
	public boolean copyFrom(OMVRBTreeEntryDataProvider<K, V> iSource);

	public boolean isEntryDirty();

	public void save();

	public void delete();

	/** SPEED UP MEMORY CLAIM BY RESETTING INTERNAL FIELDS */
	public void clear();

}
