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
import java.util.Arrays;
import java.util.Set;

import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

/**
 * 
 * Serialized as:
 * <table>
 * <tr>
 * <td>FROM</td>
 * <td>TO</td>
 * <td>FIELD</td>
 * </tr>
 * <tr>
 * <td>00</td>
 * <td>04</td>
 * <td>PAGE SIZE</td>
 * </tr>
 * <tr>
 * <td>04</td>
 * <td>14</td>
 * <td>PARENT RID</td>
 * </tr>
 * <tr>
 * <td>14</td>
 * <td>24</td>
 * <td>LEFT RID</td>
 * </tr>
 * <tr>
 * <td>24</td>
 * <td>34</td>
 * <td>RIGHT RID</td>
 * </tr>
 * <tr>
 * <td>34</td>
 * <td>35</td>
 * <td>COLOR</td>
 * </tr>
 * <tr>
 * <td>35</td>
 * <td>37</td>
 * <td>SIZE</td>
 * </tr>
 * </table>
 * VARIABLE
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <K>
 * @param <V>
 */
@SuppressWarnings({ "unchecked", "serial" })
public abstract class OMVRBTreeEntryPersistent<K, V> extends OMVRBTreeEntry<K, V> implements OSerializableStream {
	protected OMVRBTreePersistent<K, V>				pTree;

	int[]																			serializedKeys;
	int[]																			serializedValues;

	protected ORID														parentRid;
	protected ORID														leftRid;
	protected ORID														rightRid;

	public ORecordBytesLazy										record;

	protected OMVRBTreeEntryPersistent<K, V>	parent;
	protected OMVRBTreeEntryPersistent<K, V>	left;
	protected OMVRBTreeEntryPersistent<K, V>	right;

	protected OMemoryInputStream							inStream	= new OMemoryInputStream();

	/**
	 * Called on event of splitting an entry.
	 * 
	 * @param iParent
	 *          Parent node
	 * @param iPosition
	 *          Current position
	 * @param iLeft
	 */
	public OMVRBTreeEntryPersistent(final OMVRBTreeEntry<K, V> iParent, final int iPosition) {
		super(iParent, iPosition);
		pTree = (OMVRBTreePersistent<K, V>) tree;
		record = new ORecordBytesLazy(this);

		setParent(iParent);

		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();

		pageSize = pTree.getPageSize();

		// COPY ALSO THE SERIALIZED KEYS/VALUES
		serializedKeys = new int[pageSize];
		serializedValues = new int[pageSize];

		final OMVRBTreeEntryPersistent<K, V> p = (OMVRBTreeEntryPersistent<K, V>) iParent;

		inStream.setSource(p.inStream.copy());

		System.arraycopy(p.serializedKeys, iPosition, serializedKeys, 0, size);
		System.arraycopy(p.serializedValues, iPosition, serializedValues, 0, size);

		Arrays.fill(p.serializedKeys, iPosition, p.pageSize, 0);
		Arrays.fill(p.serializedValues, iPosition, p.pageSize, 0);

		markDirty();
		
		pTree.addNodeAsEntrypoint(this);
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
	public OMVRBTreeEntryPersistent(final OMVRBTreePersistent<K, V> iTree, final OMVRBTreeEntryPersistent<K, V> iParent,
			final ORID iRecordId) throws IOException {
		super(iTree);
		pTree = iTree;
		record = new ORecordBytesLazy(this);
		record.setIdentity((ORecordId) iRecordId);

		parent = iParent;
		parentRid = iParent == null ? ORecordId.EMPTY_RECORD_ID : parent.record.getIdentity();
		
		load();
		pTree.addNodeAsEntrypoint(this);
	}

	public OMVRBTreeEntryPersistent(final OMVRBTreePersistent<K, V> iTree, final K key, final V value,
			final OMVRBTreeEntryPersistent<K, V> iParent) {
		super(iTree, key, value, iParent);
		pTree = iTree;

		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();

		record = new ORecordBytesLazy(this);

		pageSize = pTree.getPageSize();

		serializedKeys = new int[pageSize];
		serializedValues = new int[pageSize];

		tree.getListener().signalNodeChanged(this);
		pTree.addNodeAsEntrypoint(this);
	}

	protected abstract Object keyFromStream(final int iIndex) throws IOException;

	protected abstract Object valueFromStream(final int iIndex) throws IOException;

	public OMVRBTreeEntryPersistent<K, V> load() throws IOException {
		return this;
	}

	/**
	 * Assures that all the links versus parent, left and right are consistent.
	 * 
	 */
	public OMVRBTreeEntryPersistent<K, V> save() throws OSerializationException {
		if (!record.isDirty())
			return this;

		final boolean isNew = record.getIdentity().isNew();

		// toStream();

		if (record.isDirty()) {
			// SAVE IF IT'S DIRTY YET
			record.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());

			if (record.getDatabase() == null) {
				throw new IllegalStateException(
						"Current thread has no database setted and the tree can't be saved correctly. Assure to close the database before the application if off.");
			}

			record.save(pTree.getClusterName());
		}

		// RE-ASSIGN RID
		if (isNew) {
			final ORecordId rid = (ORecordId) record.getIdentity();

			if (left != null) {
				left.parentRid = rid;
				left.markDirty();
			}

			if (right != null) {
				right.parentRid = rid;
				right.markDirty();
			}

			if (parent != null) {
				parentRid = parent.record.getIdentity();
				if (parent.left == this)
					parent.leftRid = rid;
				else if (parent.right == this)
					parent.rightRid = rid;
				parent.markDirty();
			}
		}
		return this;
	}

	public OMVRBTreeEntryPersistent<K, V> delete() throws IOException {
		pTree.removeNodeFromMemory(this);

		pTree.removeEntry(record.getIdentity());
		return this;
	}

	/**
	 * Disconnect the current node from others.
	 * 
	 * @param iForceDirty
	 *          Force disconnection also if the record it's dirty
	 * @param i
	 */
	protected int disconnect(final boolean iForceDirty, final int iLevel) {
		if (record == null)
			// DIRTY NODE, JUST REMOVE IT
			return 1;

		int totalDisconnected = 0;

		final ORID rid = record.getIdentity();

		if ((!record.isDirty() || iForceDirty) && !pTree.isNodeEntryPoint(this)) {
			totalDisconnected = 1;
			pTree.removeNodeFromMemory(this);
			clear();
		}

		if (parent != null) {
			// DISCONNECT RECURSIVELY THE PARENT NODE
			if (parent.left == this) {
				parent.left = null;
			} else if (parent.right == this) {
				parent.right = null;
			} else
				OLogManager.instance().warn(this,
						"Node " + rid + " has the parent (" + parent + ") unlinked to itself. It links to " + parent);

			totalDisconnected += parent.disconnect(iForceDirty, iLevel + 1);

			parent = null;
		}

		if (left != null) {
			// DISCONNECT RECURSIVELY THE LEFT NODE
			if (left.parent == this)
				left.parent = null;
			else
				OLogManager.instance().warn(this,
						"Node " + rid + " has the left (" + left + ") unlinked to itself. It links to " + left.parent);

			totalDisconnected += left.disconnect(iForceDirty, iLevel + 1);
			left = null;
		}

		if (right != null) {
			// DISCONNECT RECURSIVELY THE RIGHT NODE
			if (right.parent == this)
				right.parent = null;
			else
				OLogManager.instance().warn(this,
						"Node " + rid + " has the right (" + right + ")unlinked to itself. It links to " + right.parent);

			totalDisconnected += right.disconnect(iForceDirty, iLevel + 1);
			right = null;
		}

		return totalDisconnected;
	}

	public void clear() {
		// SPEED UP MEMORY CLAIM BY RESETTING INTERNAL FIELDS
		keys = null;
		values = null;
		if (inStream != null) {
			inStream.close();
			inStream = null;
		}
		serializedKeys = null;
		serializedValues = null;
		pTree = null;
		tree = null;
		record.recycle(null);
		record = null;
		size = 0;
	}

	/**
	 * Clear links and current node only if it's not an entry point.
	 * 
	 * @param iForceDirty
	 * 
	 * @param iSource
	 */
	protected int disconnectLinked(final boolean iForce) {
		return disconnect(iForce, 0);
	}

	public int getDepthInMemory() {
		int level = 0;
		OMVRBTreeEntryPersistent<K, V> entry = this;
		while (entry.parent != null) {
			level++;
			entry = entry.parent;
		}
		return level;
	}

	@Override
	public int getDepth() {
		int level = 0;
		OMVRBTreeEntryPersistent<K, V> entry = this;
		while (entry.getParent() != null) {
			level++;
			entry = (OMVRBTreeEntryPersistent<K, V>) entry.getParent();
		}
		return level;
	}

	@Override
	public OMVRBTreeEntry<K, V> getParent() {
		if (parentRid == null)
			return null;

		if (parent == null && parentRid.isValid()) {
			try {
				// System.out.println("Node " + record.getIdentity() + " is loading PARENT node " + parentRid + "...");

				// LAZY LOADING OF THE PARENT NODE
				parent = pTree.loadEntry(null, parentRid);

				checkEntryStructure();

				if (parent != null) {
					// TRY TO ASSIGN IT FOLLOWING THE RID
					if (parent.leftRid.isValid() && parent.leftRid.equals(record.getIdentity()))
						parent.left = this;
					else if (parent.rightRid.isValid() && parent.rightRid.equals(record.getIdentity()))
						parent.right = this;
					else {
						OLogManager.instance().error(this, "getParent: Can't assign node %s to parent. Nodes parent-left=%s, parent-right=%s",
								parentRid, parent.leftRid, parent.rightRid);
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
	public OMVRBTreeEntry<K, V> setParent(final OMVRBTreeEntry<K, V> iParent) {
		if (iParent != parent) {
			this.parent = (OMVRBTreeEntryPersistent<K, V>) iParent;

			if (parent != null && !parent.record.getIdentity().equals(parentRid))
				markDirty();

			this.parentRid = iParent == null ? ORecordId.EMPTY_RECORD_ID : parent.record.getIdentity();

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
		}
		return iParent;
	}

	@Override
	public OMVRBTreeEntry<K, V> getLeft() {
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
	public void setLeft(final OMVRBTreeEntry<K, V> iLeft) {
		if (iLeft != left) {
			left = (OMVRBTreeEntryPersistent<K, V>) iLeft;

			if (left != null && !left.record.getIdentity().equals(leftRid))
				markDirty();

			leftRid = iLeft == null ? ORecordId.EMPTY_RECORD_ID : left.record.getIdentity();

			if (left != null && left.parent != this)
				left.setParent(this);

			checkEntryStructure();
		}
	}

	@Override
	public OMVRBTreeEntry<K, V> getRight() {
		if (rightRid.isValid() && right == null) {
			// LAZY LOADING OF THE RIGHT LEAF
			try {
				right = pTree.loadEntry(this, rightRid);

				checkEntryStructure();

			} catch (IOException e) {
				OLogManager.instance().error(this, "getRight: Can't load tree. The tree could be invalid.", e, ODatabaseException.class);
			}
		}
		return right;
	}

	@Override
	public OMVRBTreeEntry<K, V> setRight(final OMVRBTreeEntry<K, V> iRight) {
		if (iRight != right) {
			right = (OMVRBTreeEntryPersistent<K, V>) iRight;

			if (right != null && !right.record.getIdentity().equals(rightRid))
				markDirty();

			rightRid = iRight == null ? ORecordId.EMPTY_RECORD_ID : right.record.getIdentity();

			if (right != null && right.parent != this)
				right.setParent(this);

			checkEntryStructure();
		}
		return right;
	}

	public void checkEntryStructure() {
		if (!tree.isRuntimeCheckEnabled())
			return;

		if (parentRid == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has parentRid null!\n", this);
		if (leftRid == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has leftRid null!\n", this);
		if (rightRid == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has rightRid null!\n", this);

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
	protected void copyFrom(final OMVRBTreeEntry<K, V> iSource) {
		markDirty();

		final OMVRBTreeEntryPersistent<K, V> source = (OMVRBTreeEntryPersistent<K, V>) iSource;

		parent = source.parent;
		left = source.left;
		right = source.right;

		parentRid = source.parentRid;
		leftRid = source.leftRid;
		rightRid = source.rightRid;

		serializedKeys = new int[source.serializedKeys.length];
		for (int i = 0; i < source.serializedKeys.length; ++i)
			serializedKeys[i] = source.serializedKeys[i];

		serializedValues = new int[source.serializedValues.length];
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

		serializedKeys[iPosition] = 0;
		serializedValues[iPosition] = 0;

		super.insert(iPosition, key, value);

		if (iPosition == 0)
			pTree.updateEntryPoint(keys[1], this);
	}

	@Override
	protected void remove() {
		markDirty();

		final int index = tree.getPageIndex();

		final K oldKey = index == 0 ? getKeyAt(0) : null;

		if (index == size - 1) {
			// LAST ONE: JUST REMOVE IT
		} else if (index > -1) {
			// SHIFT LEFT THE VALUES
			System.arraycopy(serializedKeys, index + 1, serializedKeys, index, size - index - 1);
			System.arraycopy(serializedValues, index + 1, serializedValues, index, size - index - 1);
		}

		// FREE RESOURCES
		serializedKeys[size - 1] = 0;
		serializedValues[size - 1] = 0;

		super.remove();

		if (index == 0)
			pTree.updateEntryPoint(oldKey, this);
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
				OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.unserializeKey", 1);

				keys[iIndex] = (K) keyFromStream(iIndex);
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
				OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.unserializeValue", 1);

				values[iIndex] = (V) valueFromStream(iIndex);
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
		serializedValues[tree.getPageIndex()] = -1;
		return oldValue;
	}

	public int getMaxDepthInMemory() {
		return getMaxDepthInMemory(0);
	}

	private int getMaxDepthInMemory(final int iCurrDepthLevel) {
		int depth;

		if (left != null)
			// GET THE LEFT'S DEPTH LEVEL AS GOOD
			depth = left.getMaxDepthInMemory(iCurrDepthLevel + 1);
		else
			// GET THE CURRENT DEPTH LEVEL AS GOOD
			depth = iCurrDepthLevel;

		if (right != null) {
			int rightDepth = right.getMaxDepthInMemory(iCurrDepthLevel + 1);
			if (rightDepth > depth)
				depth = rightDepth;
		}

		return depth;
	}

	/**
	 * Returns the successor of the current Entry only by traversing the memory, or null if no such.
	 */
	@Override
	public OMVRBTreeEntryPersistent<K, V> getNextInMemory() {
		OMVRBTreeEntryPersistent<K, V> t = this;
		OMVRBTreeEntryPersistent<K, V> p = null;

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

	public final OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		inStream.setSource(iStream);

		try {
			pageSize = inStream.getAsInteger();

			parentRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			leftRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			rightRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			color = inStream.getAsBoolean();
			init();
			size = inStream.getAsInteger();

			if (size > pageSize)
				throw new OConfigurationException("Loaded index with page size setted to " + pageSize
						+ " while the loaded was built with: " + size);

			// UNCOMPACT KEYS SEPARATELY
			serializedKeys = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedKeys[i] = inStream.getAsByteArrayOffset();
			}

			// KEYS WILL BE LOADED LAZY
			keys = (K[]) new Object[pageSize];

			// UNCOMPACT VALUES SEPARATELY
			serializedValues = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedValues[i] = inStream.getAsByteArrayOffset();
			}

			// VALUES WILL BE LOADED LAZY
			values = (V[]) new Object[pageSize];

			return this;
		} catch (IOException e) {
			throw new OSerializationException("Can't unmarshall RB+Tree node", e);
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeEntryP.fromStream", timer);
		}
	}

	public final byte[] toStream() throws OSerializationException {
		record.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());

		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		final Set<Integer> marshalledRecords = OSerializationThreadLocal.INSTANCE.get();
		if (marshalledRecords.contains(identityRecord)) {
			// ALREADY IN STACK, RETURN EMPTY
			return new byte[] {};
		} else
			marshalledRecords.add(identityRecord);

		if (parent != null && parentRid.isNew()) {
			// FORCE DIRTY
			parent.record.setDirty();

			parent.save();
			parentRid = parent.record.getIdentity();
			record.setDirty();
		}

		if (left != null && leftRid.isNew()) {
			// FORCE DIRTY
			left.record.setDirty();

			left.save();
			leftRid = left.record.getIdentity();
			record.setDirty();
		}

		if (right != null && rightRid.isNew()) {
			// FORCE DIRTY
			right.record.setDirty();

			right.save();
			rightRid = right.record.getIdentity();
			record.setDirty();
		}

		final long timer = OProfiler.getInstance().startChrono();

		final OMemoryOutputStream outStream = new OMemoryOutputStream();

		try {
			outStream.add(pageSize);

			outStream.addAsFixed(parentRid.toStream());
			outStream.addAsFixed(leftRid.toStream());
			outStream.addAsFixed(rightRid.toStream());

			outStream.add(color);
			outStream.add(size);

			for (int i = 0; i < size; ++i)
				serializedKeys[i] = outStream.add(serializeNewKey(i));

			for (int i = 0; i < size; ++i)
				serializedValues[i] = outStream.add(serializeNewValue(i));

			outStream.flush();

			final byte[] buffer = outStream.getByteArray();

			inStream.setSource(buffer);

			record.fromStream(buffer);
			return buffer;

		} catch (IOException e) {
			throw new OSerializationException("Can't marshall RB+Tree node", e);
		} finally {
			marshalledRecords.remove(identityRecord);

			checkEntryStructure();

			OProfiler.getInstance().stopChrono("OMVRBTreeEntryP.toStream", timer);
		}
	}

	/**
	 * Serialize only the new keys or the changed.
	 * 
	 * @return
	 * 
	 * @throws IOException
	 */
	private byte[] serializeNewKey(final int iIndex) throws IOException {
		if (serializedKeys[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.serializeValue", 1);
			return pTree.keySerializer.toStream(null, keys[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return inStream.getAsByteArray(serializedKeys[iIndex]);
	}

	/**
	 * Serialize only the new values or the changed.
	 * 
	 * @throws IOException
	 */
	private byte[] serializeNewValue(final int iIndex) throws IOException {
		if (serializedValues[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.serializeKey", 1);
			return pTree.valueSerializer.toStream(null, values[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return inStream.getAsByteArray(serializedValues[iIndex]);
	}

	@Override
	protected void setColor(final boolean iColor) {
		if (iColor == color)
			return;

		markDirty();
		super.setColor(iColor);
	}

	void markDirty() {
		if (record == null || record.isDirty())
			return;

		record.setDirty();
		tree.getListener().signalNodeChanged(this);
	}

	@Override
	protected OMVRBTreeEntry<K, V> getLeftInMemory() {
		return left;
	}

	@Override
	protected OMVRBTreeEntry<K, V> getParentInMemory() {
		return parent;
	}

	@Override
	protected OMVRBTreeEntry<K, V> getRightInMemory() {
		return right;
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
