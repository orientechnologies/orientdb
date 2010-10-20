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
package com.orientechnologies.orient.core.type.tree;

import java.io.IOException;
import java.util.Set;

import com.orientechnologies.common.collection.OTreeMapEntry;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

@SuppressWarnings("unchecked")
public abstract class OTreeMapEntryPersistent<K, V> extends OTreeMapEntry<K, V> implements OSerializableStream {
	protected OTreeMapPersistent<K, V>			pTree;

	byte[][]																serializedKeys;
	byte[][]																serializedValues;

	protected ORID													parentRid;
	protected ORID													leftRid;
	protected ORID													rightRid;

	public ORecordBytes											record;

	protected OTreeMapEntryPersistent<K, V>	parent;
	protected OTreeMapEntryPersistent<K, V>	left;
	protected OTreeMapEntryPersistent<K, V>	right;

	/**
	 * Called on event of splitting an entry.
	 * 
	 * @param iParent
	 *          Parent node
	 * @param iPosition
	 *          Current position
	 * @param iLeft
	 */
	public OTreeMapEntryPersistent(final OTreeMapEntry<K, V> iParent, final int iPosition) {
		super(iParent, iPosition);
		pTree = (OTreeMapPersistent<K, V>) tree;
		record = new ORecordBytes();

		setParent(iParent);

		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();

		pageSize = pTree.getPageSize();

		// COPY ALSO THE SERIALIZED KEYS/VALUES
		serializedKeys = new byte[pageSize][];
		serializedValues = new byte[pageSize][];

		final OTreeMapEntryPersistent<K, V> p = (OTreeMapEntryPersistent<K, V>) iParent;

		System.arraycopy(p.serializedKeys, iPosition, serializedKeys, 0, size);
		System.arraycopy(p.serializedValues, iPosition, serializedValues, 0, size);

		markDirty();
	}

	/**
	 * Called upon unmarshalling.
	 * 
	 * @param iTree
	 *          Tree which belong
	 * @param iParent
	 *          Parent node if any
	 * @param iRecordId
	 *          Record to unmarshall
	 */
	public OTreeMapEntryPersistent(final OTreeMapPersistent<K, V> iTree, final OTreeMapEntryPersistent<K, V> iParent,
			final ORID iRecordId) throws IOException {
		super(iTree);
		pTree = iTree;
		record = new ORecordBytes();
		record.setIdentity(iRecordId.getClusterId(), iRecordId.getClusterPosition());
		setParent(iParent);
	}

	public OTreeMapEntryPersistent(final OTreeMapPersistent<K, V> iTree, final K key, final V value,
			final OTreeMapEntryPersistent<K, V> iParent) {
		super(iTree, key, value, iParent);
		pTree = iTree;

		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();

		record = new ORecordBytes();

		pageSize = pTree.getPageSize();

		serializedKeys = new byte[pageSize][];
		serializedValues = new byte[pageSize][];

		markDirty();
	}

	public OTreeMapEntryPersistent<K, V> load() throws IOException {
		return this;
	}

	public OTreeMapEntryPersistent<K, V> save() throws IOException {
		return this;
	}

	public OTreeMapEntryPersistent<K, V> delete() throws IOException {
		pTree.removeEntryPoint(this);
		pTree.cache.remove(record.getIdentity());

		// DELETE THE NODE FROM THE PENDING RECORDS TO COMMIT
		/*
		 * OTreeMapEntryPersistent<K, V> node; for (int i = 0; i < pTree.recordsToCommit.size(); ++i) { node =
		 * pTree.recordsToCommit.get(i); if( node.record.getIdentity().equals( record.getIdentity() )) {
		 * pTree.recordsToCommit.remove(i); --i; } }
		 */
		for (OTreeMapEntryPersistent<K, V> node : pTree.recordsToCommit) {
			if (node.record.getIdentity().equals(record.getIdentity())) {
				pTree.recordsToCommit.remove(node);
			}
		}
		return this;
	}

	/**
	 * Disconnect the current node from others.
	 */
	protected int disconnectLinked(final boolean iForceDirty) {
		int disconnected = 0;

		if (parent != null) {
			if (parent.left == this) {
				parent.left = null;
				parent.leftRid = ORecordId.EMPTY_RECORD_ID;
			} else {
				parent.right = null;
				parent.rightRid = ORecordId.EMPTY_RECORD_ID;
			}
			disconnected += parent.disconnectLinked(iForceDirty);
			parent = null;
			parentRid = ORecordId.EMPTY_RECORD_ID;
		}

		if (left != null) {
			// DISCONNECT MYSELF FROM THE LEFT NODE
			left.parent = null;
			left.parentRid = ORecordId.EMPTY_RECORD_ID;
			disconnected += left.disconnectLinked(iForceDirty);
			left = null;
			leftRid = ORecordId.EMPTY_RECORD_ID;
		}

		if (right != null) {
			// DISCONNECT MYSELF FROM THE RIGHT NODE
			right.parent = null;
			right.parentRid = ORecordId.EMPTY_RECORD_ID;
			disconnected += right.disconnectLinked(iForceDirty);
			right = null;
			rightRid = ORecordId.EMPTY_RECORD_ID;
		}

		return disconnected;
	}

	/**
	 * Clear links and current node only if it's not an entry point.
	 * 
	 * @param iForceDirty
	 * 
	 * @param iSource
	 */
	protected int disconnect(final int iDirection, boolean iForceDirty) {
		if (record == null || this == pTree.getRoot())
			// DIRTY NODE OR IS ROOT
			return 0;

		if (!iForceDirty && record.isDirty())
			return 0;

		boolean entryToKeep = false;
		// CHECK IF IT'S PART OF ENTRYPOINTS
		for (OTreeMapEntryPersistent<K, V> e : pTree.entryPoints)
			if (e == this) {
				entryToKeep = true;
				break;
			}

		if (!iForceDirty && !entryToKeep)
			// CHECK IF IT'S PART OF THE RECORDS TO COMMIT. CHECK IF IT'S DIRTY IS NOT ENOUGH
			entryToKeep = pTree.recordsToCommit.contains(this);

		if (!entryToKeep) {
			keys = null;
			values = null;
			serializedKeys = null;
			serializedValues = null;
			record = null;
			tree = pTree = null;
		}

		return disconnectLinked(iForceDirty) + 1;
	}

	@Override
	public int getDepth() {
		int level = 0;
		OTreeMapEntryPersistent<K, V> entry = this;
		while (entry.parent != null) {
			level++;
			entry = (OTreeMapEntryPersistent<K, V>) entry.parent;
		}
		return level;
	}

	@Override
	public OTreeMapEntry<K, V> getParent() {
		if (parentRid == null)
			return null;

		if (parent == null && parentRid.isValid()) {
			try {
				// System.out.println("Node " + record.getIdentity() + " is loading PARENT node " + parentRid + "...");

				// LAZY LOADING OF THE PARENT NODE
				parent = pTree.loadEntry(null, parentRid);

				checkEntryStructure();

				if (parent != null) {
					// if (!parent.record.isDirty())
					// parent.load();

					// TRY TO ASSIGN IT FOLLOWING THE RID
					if (parent.leftRid.isValid() && parent.leftRid.equals(record.getIdentity()))
						parent.left = this;
					else if (parent.rightRid.isValid() && parent.rightRid.equals(record.getIdentity()))
						parent.right = this;
					else {
						OLogManager.instance().error(this, "getParent: Can't assign node %s to parent. Nodes parent-left=%s, parent-right=%s",
								parentRid, parent.leftRid, parent.rightRid);

						parent.load();
					}
				}

			} catch (IOException e) {
				OLogManager.instance().error(this, "getParent: Can't load the tree. The tree could be invalid.", e,
						ODatabaseException.class);
			}
		}
		return parent;
	}

	@Override
	public OTreeMapEntry<K, V> setParent(final OTreeMapEntry<K, V> iParent) {
		if (iParent != getParent()) {
			markDirty();

			this.parent = (OTreeMapEntryPersistent<K, V>) iParent;
			this.parentRid = iParent == null ? ORecordId.EMPTY_RECORD_ID : new ORecordId(parent.record.getIdentity());

			if (parent != null) {
				if (parent.left == this && !parent.leftRid.equals(record.getIdentity()))
					parent.leftRid = record.getIdentity();
				if (parent.left != this && parent.leftRid.isValid() && parent.leftRid.equals(record.getIdentity()))
					parent.left = this;
				if (parent.right == this && !parent.rightRid.equals(record.getIdentity()))
					parent.rightRid = record.getIdentity();
				if (parent.right != this && parent.rightRid.isValid() && parent.rightRid.equals(record.getIdentity()))
					parent.right = this;
			}
			checkEntryStructure();
		}
		return iParent;
	}

	@Override
	public OTreeMapEntry<K, V> getLeft() {
		if (left == null && leftRid.isValid()) {
			try {
				// System.out.println("Node " + record.getIdentity() + " is loading LEFT node " + leftRid + "...");

				// LAZY LOADING OF THE LEFT LEAF
				left = pTree.loadEntry(this, leftRid);

				checkEntryStructure();

			} catch (IOException e) {
				OLogManager.instance().error(this, "getLeft: Can't load the tree. The tree could be invalid.", e, ODatabaseException.class);
			}
		}
		return left;
	}

	@Override
	public void setLeft(final OTreeMapEntry<K, V> iLeft) {
		if (iLeft == left)
			return;

		left = (OTreeMapEntryPersistent<K, V>) iLeft;
		// if (left == null || !left.record.getIdentity().isValid() || !left.record.getIdentity().equals(leftRid)) {
		markDirty();
		this.leftRid = iLeft == null ? ORecordId.EMPTY_RECORD_ID : new ORecordId(left.record.getIdentity());
		// }

		if (iLeft != null && iLeft.getParent() != this)
			iLeft.setParent(this);

		checkEntryStructure();
	}

	@Override
	public OTreeMapEntry<K, V> getRight() {
		if (rightRid.isValid() && right == null) {
			// LAZY LOADING OF THE RIGHT LEAF
			try {
				// System.out.println("Node " + record.getIdentity() + " is loading RIGHT node " + rightRid + "...");

				right = pTree.loadEntry(this, rightRid);

				checkEntryStructure();

			} catch (IOException e) {
				OLogManager.instance().error(this, "getRight: Can't load tree. The tree could be invalid.", e, ODatabaseException.class);
			}
		}
		return right;
	}

	@Override
	public OTreeMapEntry<K, V> setRight(final OTreeMapEntry<K, V> iRight) {
		if (iRight == right)
			return this;

		right = (OTreeMapEntryPersistent<K, V>) iRight;
		// if (right == null || !right.record.getIdentity().isValid() || !right.record.getIdentity().equals(rightRid)) {
		markDirty();
		rightRid = iRight == null ? ORecordId.EMPTY_RECORD_ID : new ORecordId(right.record.getIdentity());
		// }

		if (iRight != null && iRight.getParent() != this)
			iRight.setParent(this);

		checkEntryStructure();

		return right;
	}

	public void checkEntryStructure() {
		if (!tree.isRuntimeCheckEnabled())
			return;

		if (this == left || record.getIdentity().isValid() && record.getIdentity().equals(leftRid))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left that points to itself!\n", this);
		if (this == right || record.getIdentity().isValid() && record.getIdentity().equals(rightRid))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has right that points to itself!\n", this);
		if (left != null && left == right)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left and right equals!\n", this);

		if (left != null) {
			if (!left.record.getIdentity().equals(leftRid))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong left node loaded: " + leftRid);
			// if (left.parent != this)
			// OLogManager.instance().error(this, "checkEntryStructure: Left node is not correctly connected to the parent" + leftRid);
		}

		if (right != null) {
			if (!right.record.getIdentity().equals(rightRid))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong right node loaded: " + rightRid);
			// if (right.parent != this)
			// OLogManager.instance().error(this, "checkEntryStructure: Right node is not correctly connected to the parent" + leftRid);
		}
	}

	@Override
	protected void copyFrom(final OTreeMapEntry<K, V> iSource) {
		markDirty();

		final OTreeMapEntryPersistent<K, V> source = (OTreeMapEntryPersistent<K, V>) iSource;

		parent = source.parent;
		left = source.left;
		right = source.right;

		parentRid = new ORecordId(source.parentRid);
		leftRid = new ORecordId(source.leftRid);
		rightRid = new ORecordId(source.rightRid);

		serializedKeys = new byte[source.serializedKeys.length][];
		for (int i = 0; i < source.serializedKeys.length; ++i)
			serializedKeys[i] = source.serializedKeys[i];

		serializedValues = new byte[source.serializedValues.length][];
		for (int i = 0; i < source.serializedValues.length; ++i)
			serializedValues[i] = source.serializedValues[i];

		super.copyFrom(source);
	}

	@Override
	protected void insert(final int iPosition, final K key, final V value) {
		markDirty();

		if (iPosition < size) {
			System.arraycopy(serializedKeys, iPosition, serializedKeys, iPosition + 1, size - iPosition);
			System.arraycopy(serializedValues, iPosition, serializedValues, iPosition + 1, size - iPosition);
		}

		serializedKeys[iPosition] = null;
		serializedValues[iPosition] = null;

		super.insert(iPosition, key, value);
	}

	@Override
	protected void remove() {
		markDirty();

		final int index = tree.getPageIndex();

		if (index == size - 1) {
			// LAST ONE: JUST REMOVE IT
		} else if (index > -1) {
			// SHIFT LEFT THE VALUES
			System.arraycopy(serializedKeys, index + 1, serializedKeys, index, size - index - 1);
			System.arraycopy(serializedValues, index + 1, serializedValues, index, size - index - 1);
		}

		// FREE RESOURCES
		serializedKeys[size - 1] = null;
		serializedValues[size - 1] = null;

		super.remove();
	}

	/**
	 * Return the key. Keys are lazy loaded.
	 * 
	 * @param iIndex
	 * @return
	 */
	@Override
	public K getKeyAt(final int iIndex) {
		if (keys[iIndex] == null)
			try {
				OProfiler.getInstance().updateCounter("OTreeMapEntryP.unserializeKey", 1);

				keys[iIndex] = (K) pTree.keySerializer.fromStream(serializedKeys[iIndex]);
			} catch (IOException e) {

				OLogManager.instance().error(this, "Can't lazy load the key #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return keys[iIndex];
	}

	@Override
	protected V getValueAt(final int iIndex) {
		if (values[iIndex] == null)
			try {
				OProfiler.getInstance().updateCounter("OTreeMapEntryP.unserializeValue", 1);

				values[iIndex] = (V) pTree.valueSerializer.fromStream(serializedValues[iIndex]);
			} catch (IOException e) {

				OLogManager.instance().error(this, "Can't lazy load the value #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return values[iIndex];
	}

	/**
	 * Invalidate serialized Value associated in order to be re-marshalled on the next node storing.
	 */
	@Override
	public V setValue(final V value) {
		markDirty();

		V oldValue = super.setValue(value);
		serializedValues[tree.getPageIndex()] = null;
		return oldValue;
	}

	/**
	 * Returns the successor of the current Entry only by traversing the memory, or null if no such.
	 */
	public OTreeMapEntryPersistent<K, V> getNextInMemory() {
		OTreeMapEntryPersistent<K, V> t = this;
		OTreeMapEntryPersistent<K, V> p = null;

		if (t.right != null) {
			p = t.right;
			while (p.left != null)
				p = p.left;
		} else {
			p = t.parent;
			while (p != null && t == p.right) {
				t = p;
				p = p.parent;
			}
		}

		return p;
	}

	public final OSerializableStream fromStream(final byte[] iStream) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		final OMemoryInputStream buffer = new OMemoryInputStream(iStream);

		try {
			pageSize = buffer.getAsShort();

			parentRid = new ORecordId().fromStream(buffer.getAsByteArray());
			leftRid = new ORecordId().fromStream(buffer.getAsByteArray());
			rightRid = new ORecordId().fromStream(buffer.getAsByteArray());

			color = buffer.getAsBoolean();
			init();
			size = buffer.getAsShort();

			if (size > pageSize)
				throw new OConfigurationException("Loaded index with page size setted to " + pageSize
						+ " while the loaded was built with: " + size);

			// UNCOMPACT KEYS SEPARATELY
			serializedKeys = new byte[pageSize][];
			for (int i = 0; i < size; ++i) {
				serializedKeys[i] = buffer.getAsByteArray();
			}

			// KEYS WILL BE LOADED LAZY
			keys = (K[]) new Object[pageSize];

			// UNCOMPACT VALUES SEPARATELY
			serializedValues = new byte[pageSize][];
			for (int i = 0; i < size; ++i) {
				serializedValues[i] = buffer.getAsByteArray();
			}

			// VALUES WILL BE LOADED LAZY
			values = (V[]) new Object[pageSize];

			return this;
		} finally {
			buffer.close();

			OProfiler.getInstance().stopChrono("OTreeMapEntryP.fromStream", timer);
		}
	}

	public final byte[] toStream() throws IOException {
		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		final Set<Integer> marshalledRecords = OSerializationThreadLocal.INSTANCE.get();
		if (marshalledRecords.contains(identityRecord)) {
			// ALREADY IN STACK, RETURN EMPTY
			return new byte[] {};
		} else
			marshalledRecords.add(identityRecord);

		final long timer = OProfiler.getInstance().startChrono();

		OMemoryOutputStream stream = pTree.entryRecordBuffer;

		try {
			stream.add((short) pageSize);

			stream.add(parentRid.toStream());
			stream.add(leftRid.toStream());
			stream.add(rightRid.toStream());

			stream.add(color);
			stream.add((short) size);

			serializeNewKeys();
			serializeNewValues();

			for (int i = 0; i < size; ++i)
				stream.add(serializedKeys[i]);

			for (int i = 0; i < size; ++i)
				stream.add(serializedValues[i]);

			stream.flush();

			record.fromStream(stream.getByteArray());
			return record.toStream();
		} finally {
			stream.close();

			marshalledRecords.remove(identityRecord);

			OProfiler.getInstance().stopChrono("OTreeMapEntryP.toStream", timer);
		}
	}

	/**
	 * Serialize only the new keys or the changed.
	 * 
	 * @throws IOException
	 */
	private void serializeNewKeys() throws IOException {
		for (int i = 0; i < size; ++i) {
			if (serializedKeys[i] == null) {
				OProfiler.getInstance().updateCounter("OTreeMapEntryP.serializeValue", 1);

				serializedKeys[i] = pTree.keySerializer.toStream(keys[i]);
			}
		}
	}

	/**
	 * Serialize only the new values or the changed.
	 * 
	 * @throws IOException
	 */
	private void serializeNewValues() throws IOException {
		for (int i = 0; i < size; ++i) {
			if (serializedValues[i] == null) {
				OProfiler.getInstance().updateCounter("OTreeMapEntryP.serializeKey", 1);

				serializedValues[i] = pTree.valueSerializer.toStream(values[i]);
			}
		}
	}

	@Override
	protected void setColor(final boolean iColor) {
		if (iColor == color)
			return;

		markDirty();
		super.setColor(iColor);
	}

	private void markDirty() {
		if (record == null || record.isDirty())
			return;

		record.setDirty();
		tree.getListener().signalNodeChanged(this);
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (!(o instanceof OTreeMapEntryPersistent<?, ?>))
			return false;

		final OTreeMapEntryPersistent<?, ?> e = (OTreeMapEntryPersistent<?, ?>) o;

		if (record != null && e.record != null)
			return record.getIdentity().equals(e.record.getIdentity());

		return false;
	}

	@Override
	public int hashCode() {
		final ORID rid = record.getIdentity();
		return rid == null ? 0 : rid.hashCode();
	}

	@Override
	protected OTreeMapEntry<K, V> getLeftInMemory() {
		return left;
	}

	@Override
	protected OTreeMapEntry<K, V> getParentInMemory() {
		return parent;
	}

	@Override
	protected OTreeMapEntry<K, V> getRightInMemory() {
		return right;
	}

	/**
	 * Assure that all the links versus parent, left and right are consistent.
	 * 
	 * @param iMarshalledRecords
	 */
	protected boolean assureIntegrityOfReferences() throws IOException {
		if (parent != null && !parentRid.isValid()) {
			if (parent.record.getIdentity().isNew())
				((OTreeMapEntryDatabase<K, V>) parent).save();
			parentRid = parent.record.getIdentity();
			record.setDirty();
		}

		if (left != null && !leftRid.isValid()) {
			if (left.record.getIdentity().isNew())
				((OTreeMapEntryDatabase<K, V>) left).save();
			leftRid = left.record.getIdentity();
			record.setDirty();
		}

		if (right != null && !rightRid.isValid()) {
			if (right.record.getIdentity().isNew())
				((OTreeMapEntryDatabase<K, V>) right).save();
			rightRid = right.record.getIdentity();
			record.setDirty();
		}

		if (record.isDirty()) {
			final boolean isNew = record.getIdentity().isNew();

			toStream();
			record.save(pTree.getClusterName());

			if (isNew) {
				if (left != null)
					left.parentRid = record.getIdentity();
				if (right != null)
					right.parentRid = record.getIdentity();
				if (parent != null) {
					if (parent.left == this)
						parent.leftRid = record.getIdentity();
					else if (parent.right == this)
						parent.rightRid = record.getIdentity();
				}
			}
		}

		return true;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		if (record != null && record.getIdentity().isValid())
			builder.append('@').append(record.getIdentity()).append(" ");
		builder.append(super.toString());
		return builder.toString();
	}
}
