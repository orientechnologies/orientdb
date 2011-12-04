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
 * Interface to handle persistence of a tree.
 * 
 * @author Sylvain Spinelli (sylvain.spinelli@kelis.fr)
 * 
 * @param <K>
 *          Key
 * @param <V>
 *          Value
 */
public interface OMVRBTreeProvider<K, V> {

	public int getSize();

	public int getDefaultPageSize();

	public ORID getRoot();

	public boolean setRoot(ORID iRid);

	public boolean setSize(int iSize);

	/** Give a chance to update config parameters (defaultSizePage, ...) */
	public boolean updateConfig();

	public boolean isTreeDirty();

	public OMVRBTreeEntryDataProvider<K, V> createEntry();

	public OMVRBTreeEntryDataProvider<K, V> getEntry(ORID iRid);

	public void load();

	public void save();

	public void delete();

	public int getClusterId();

	public String getClusterName();
}
