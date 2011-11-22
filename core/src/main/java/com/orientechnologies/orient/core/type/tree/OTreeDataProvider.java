package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORID;

public interface OTreeDataProvider<K, V> {

	public int getSize();

	public int getDefaultPageSize();

	public ORID getRoot();

	public boolean setSize(int iSize);

	public boolean setRoot(ORID iRid);

	/** Give a chance to update config parameters (defaultSizePage, ...) */
	public boolean updateConfig();

	public boolean isTreeDirty();

	public OTreeEntryDataProvider<K, V> createEntry();

	public OTreeEntryDataProvider<K, V> getEntry(ORID iRid);

	public void load();

	public void save();

	public void delete();

}
