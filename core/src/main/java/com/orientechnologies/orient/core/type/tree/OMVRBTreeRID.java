/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Kersion 2.0 (the "License");
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
package com.orientechnologies.orient.core.type.tree;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OLazyRecordIterator;
import com.orientechnologies.orient.core.db.record.OLazyRecordMultiIterator;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDEntryProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent MVRB-Tree Set implementation.
 * 
 */
public class OMVRBTreeRID extends OMVRBTreePersistent<OIdentifiable, OIdentifiable> {
	private IdentityHashMap<ORecord<?>, Object>	newItems;
	private boolean															autoConvertToRecord	= true;
	private Listener														watchDog;

	private static final Object									NEWMAP_VALUE				= new Object();
	private static final long										serialVersionUID		= 1L;

	public OMVRBTreeRID() {
		this(new OMVRBTreeRIDProvider(null, ODatabaseRecordThreadLocal.INSTANCE.get().getDefaultClusterId()));
	}

	public OMVRBTreeRID(final ORID iRID) {
		this(new OMVRBTreeRIDProvider(null, iRID.getClusterId(), iRID));
		load();
	}

	public OMVRBTreeRID(final String iClusterName) {
		this(new OMVRBTreeRIDProvider(null, iClusterName));
	}

	public OMVRBTreeRID(final OMVRBTreeProvider<OIdentifiable, OIdentifiable> iProvider) {
		super(iProvider);
		((OMVRBTreeRIDProvider) dataProvider).setTree(this);
		watchDog = new Listener() {
			public void memoryUsageLow(final long iFreeMemory, final long iFreeMemoryPercentage) {
				setOptimization(iFreeMemoryPercentage < 10 ? 2 : 1);
			}
		};
		Orient.instance().getMemoryWatchDog().addListener(watchDog);
	}

	/**
	 * Copy constructor
	 * 
	 * @param iSource
	 *          Source object
	 */
	public OMVRBTreeRID(final OMVRBTreeRID iSource) {
		super(new OMVRBTreeRIDProvider((OMVRBTreeRIDProvider) iSource.getProvider()));
		((OMVRBTreeRIDProvider) dataProvider).setTree(this);
		if (((OMVRBTreeRIDProvider) iSource.getProvider()).isEmbeddedStreaming())
			((OMVRBTreeRIDProvider) dataProvider).fill(iSource.keySet());
		else
			load();
	}

	@Override
	protected void finalize() throws Throwable {
		if (watchDog != null)
			Orient.instance().getMemoryWatchDog().removeListener(watchDog);
	}

	@Override
	public OMVRBTreePersistent<OIdentifiable, OIdentifiable> load() {
		newItems = null;
		super.load();
		if (root != null)
			setSize(((OMVRBTreeRIDEntryProvider) ((OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) root).getProvider())
					.getTreeSize());
		else
			setSize(0);
		return this;
	}

	@Override
	public OIdentifiable put(final OIdentifiable e, final OIdentifiable v) {
		if (e.getIdentity().isNew()) {
			final ORecord<?> record = e.getRecord();

			// ADD IN TEMP LIST
			if (newItems == null)
				newItems = new IdentityHashMap<ORecord<?>, Object>();
			else if (newItems.containsKey(record))
				return (OIdentifiable) newItems.get(record);
			newItems.put(record, NEWMAP_VALUE);
			setDirty();
			return null;
		}

		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		return super.put(e, null);
	}

	public OIdentifiable remove(final Object o) {
		final OIdentifiable removed;

		if (hasNewItems() && newItems.containsKey(o)) {
			// REMOVE IT INSIDE NEW ITEMS MAP
			removed = (OIdentifiable) o;
			newItems.remove(o);
			if (newItems.size() == 0)
				// EARLY REMOVE THE MAP TO SAVE MEMORY
				newItems = null;
			setDirty();
		} else {
			if (containsKey(o)) {
				removed = super.remove(o);
				setDirty();
			} else
				removed = null;
		}

		return removed;
	}

	public boolean removeAll(final Collection<?> c) {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();

		if (hasNewItems()) {
			final Collection<ORecord<?>> v = newItems.keySet();
			v.removeAll(c);
			if (newItems.size() == 0)
				newItems = null;
		}

		boolean modified = false;
		for (Object o : c)
			if (remove(o) != null)
				modified = true;
		return modified;
	}

	public boolean retainAll(final Collection<?> c) {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		if (hasNewItems()) {
			final Collection<ORecord<?>> v = newItems.keySet();
			v.retainAll(c);
			if (newItems.size() == 0)
				newItems = null;
		}

		boolean modified = false;
		final Iterator<?> e = iterator();
		while (e.hasNext()) {
			if (!c.contains(e.next())) {
				e.remove();
				modified = true;
			}
		}
		return modified;
	}

	@Override
	public void clear() {
		if (newItems != null) {
			newItems.clear();
			newItems = null;
		}
		setDirty();
		super.clear();
		save();
	}

	public boolean detach() {
		return saveAllNewItems();
	}

	@Override
	public int size() {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		int tot = super.size();
		if (newItems != null)
			tot += newItems.size();
		return tot;
	}

	@Override
	public boolean isEmpty() {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		boolean empty = super.isEmpty();

		if (empty && newItems != null)
			empty = newItems.isEmpty();

		return empty;
	}

	@Override
	public boolean containsKey(final Object o) {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		boolean found = super.containsKey(o);

		if (!found && hasNewItems())
			// SEARCH INSIDE NEW ITEMS MAP
			found = newItems.containsKey(o);

		return found;
	}

	public OLazyIterator<OIdentifiable> iterator() {
		return iterator(autoConvertToRecord);
	}

	public OLazyIterator<OIdentifiable> iterator(final boolean iAutoConvertToRecord) {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		if (hasNewItems())
			return new OLazyRecordMultiIterator(null, new Object[] { keySet().iterator(), newItems.keySet().iterator() },
					iAutoConvertToRecord);

		return new OLazyRecordIterator(keySet().iterator(), iAutoConvertToRecord);
	}

	@Override
	public Set<OIdentifiable> keySet() {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		return super.keySet();
	}

	@Override
	public Collection<OIdentifiable> values() {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		return super.values();
	}

	public Object[] toArray() {
		Object[] result = keySet().toArray();
		if (newItems != null && !newItems.isEmpty()) {
			int start = result.length;
			result = Arrays.copyOf(result, start + newItems.size());

			for (ORecord<?> r : newItems.keySet()) {
				result[start++] = r;
			}
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> T[] toArray(final T[] a) {
		T[] result = keySet().toArray(a);

		if (newItems != null && !newItems.isEmpty()) {
			int start = result.length;
			result = Arrays.copyOf(result, start + newItems.size());

			for (ORecord<?> r : newItems.keySet()) {
				result[start++] = (T) r;
			}
		}

		return result;
	}

	@Override
	protected void saveTreeNode() {
	}

	@Override
	public int commitChanges() {
		if (updateSize())
			return super.commitChanges();
		return 0;
	}

	public OMVRBTreePersistent<OIdentifiable, OIdentifiable> save() {
		if (((OMVRBTreeRIDProvider) dataProvider).isDirty())
			((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();

		if (saveAllNewItems())
			updateSize();
		return this;
	}

	/**
	 * Notifies to the owner the change
	 */
	public void setDirtyOwner() {
		if (getOwner() != null)
			getOwner().setDirty();
	}

	public void onAfterTxCommit() {
		final Set<ORID> nodesInMemory = getAllNodesInCache();

		if (nodesInMemory.isEmpty())
			return;

		// FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
		final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
		OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable> entry;
		for (ORID rid : keys) {
			if (rid.getClusterPosition() < -1) {
				// FIX IT IN CACHE
				entry = (OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) searchNodeInCache(rid);

				// OVERWRITE IT WITH THE NEW RID
				removeNodeFromCache(rid);
				addNodeInCache(entry);
			}
		}
	}

	public boolean saveAllNewItems() {
		if (hasNewItems()) {
			for (Iterator<ORecord<?>> it = newItems.keySet().iterator(); it.hasNext();) {
				final ORecord<?> record = it.next();

				if (record.getIdentity().isNew())
					record.save();

				if (!record.getIdentity().isNew()) {
					// INSERT ONLY PERSISTENT RIDS
					super.put(record.getIdentity(), null);
					it.remove();
				}
			}

			if (newItems.size() == 0)
				newItems = null;
		}
		return true;
	}

	public boolean hasNewItems() {
		return newItems != null && !newItems.isEmpty();
	}

	public boolean isAutoConvert() {
		return autoConvertToRecord;
	}

	public OMVRBTreeRID setAutoConvert(boolean autoConvert) {
		this.autoConvertToRecord = autoConvert;
		return this;
	}

	@Override
	public String toString() {
		((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
		final StringBuilder buffer = new StringBuilder(super.toString());
		if (hasNewItems()) {
			buffer.append(" + new items (");
			buffer.append(newItems.size());
			buffer.append("): ");
			boolean first = true;
			for (ORecord<?> item : newItems.keySet()) {
				if (!first) {
					buffer.append(", ");
					first = false;
				}

				buffer.append(item.toString());
			}
		}
		return buffer.toString();
	}

	/**
	 * Notifies the changes to the owner if it's embedded.
	 */
	@SuppressWarnings("unchecked")
	protected <RET> RET setDirty() {
		((OMVRBTreeRIDProvider) getProvider()).setDirty();

		if (((OMVRBTreeRIDProvider) getProvider()).isEmbeddedStreaming())
			setDirtyOwner();
		else if (ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().getStatus() != OTransaction.TXSTATUS.BEGUN)
			// SAVE IT RIGHT NOW SINCE IT'S DISCONNECTED FROM OWNER
			save();

		return (RET) this;
	}

	protected boolean updateSize() {
		if (!((OMVRBTreeRIDProvider) getProvider()).isEmbeddedStreaming()) {
			if (root != null) {
				if (((OMVRBTreeRIDEntryProvider) ((OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) root).getProvider())
						.setTreeSize(size()))
					((OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) root).markDirty();
				return true;
			}
		}
		return false;
	}

	public IdentityHashMap<ORecord<?>, Object> getTemporaryEntries() {
		return newItems;
	}
}
