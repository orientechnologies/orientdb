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

import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

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
public class OMVRBTreeEntryPersistent<K, V> extends OMVRBTreeEntry<K, V> {
	protected OTreeEntryDataProvider<K, V>		dataEntry;
	protected OMVRBTreePersistent<K, V>				pTree;

	protected OMVRBTreeEntryPersistent<K, V>	parent;
	protected OMVRBTreeEntryPersistent<K, V>	left;
	protected OMVRBTreeEntryPersistent<K, V>	right;

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
			final ORID iRecordId) {
		super(iTree);
		pTree = iTree;
		dataEntry = pTree.dataTree.getEntry(iRecordId);
		init();
		parent = iParent;
		// setParent(iParent);
		pTree.addNodeAsEntrypoint(this);
	}

	/**
	 * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and BLACK color.
	 */
	public OMVRBTreeEntryPersistent(final OMVRBTreePersistent<K, V> iTree, final K iKey, final V iValue,
			final OMVRBTreeEntryPersistent<K, V> iParent) {
		super(iTree);
		pTree = iTree;
		dataEntry = pTree.dataTree.createEntry();
		dataEntry.insertAt(0, iKey, iValue);
		init();
		setParent(iParent);
		pTree.addNodeAsEntrypoint(this);
		// created entry : force dispatch dirty node.
		markDirty();
	}

	/**
	 * Called on event of splitting an entry. Copy values from the parent node.
	 * 
	 * @param iParent
	 *          Parent node
	 * @param iPosition
	 *          Current position
	 */
	public OMVRBTreeEntryPersistent(final OMVRBTreeEntry<K, V> iParent, final int iPosition) {
		super(((OMVRBTreeEntryPersistent<K, V>) iParent).getTree());
		pTree = (OMVRBTreePersistent<K, V>) tree;
		OMVRBTreeEntryPersistent<K, V> pParent = (OMVRBTreeEntryPersistent<K, V>) iParent;
		dataEntry = pTree.dataTree.createEntry();
		dataEntry.copyDataFrom(pParent.dataEntry, iPosition);
		if (pParent.dataEntry.truncate(iPosition))
			pParent.markDirty();
		init();
		setParent(pParent);
		pTree.addNodeAsEntrypoint(this);
		// created entry : force dispatch dirty node.
		markDirty();
	}

	public OTreeEntryDataProvider<K, V> getDataEntry() {
		return dataEntry;
	}

	/**
	 * Assures that all the links versus parent, left and right are consistent.
	 * 
	 */
	public OMVRBTreeEntryPersistent<K, V> save() throws OSerializationException {
		if (!dataEntry.isEntryDirty())
			return this;

		final boolean isNew = dataEntry.getIdentity().isNew();

		// FOR EACH NEW LINK, SAVE BEFORE
		if (left != null && left.dataEntry.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				left.dataEntry.save();
				left.updateRefsAfterCreation();
			} else
				left.save();
		}
		if (right != null && right.dataEntry.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				right.dataEntry.save();
				right.updateRefsAfterCreation();
			} else
				right.save();
		}
		if (parent != null && parent.dataEntry.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				parent.dataEntry.save();
				parent.updateRefsAfterCreation();
			} else
				parent.save();
		}

		dataEntry.save();

		// RE-ASSIGN RID
		if (isNew)
			updateRefsAfterCreation();

		// if (parent != null)
		// if (!parent.record.getIdentity().equals(parentRid))
		// OLogManager.instance().error(this,
		// "[save]: Tree node %s has parentRid '%s' different by the rid of the assigned parent node: %s", record.getIdentity(),
		// parentRid, parent.record.getIdentity());

		checkEntryStructure();

		if (pTree.searchNodeInCache(dataEntry.getIdentity()) != this) {
			// UPDATE THE CACHE
			pTree.addNodeInCache(this);
		}

		return this;
	}

	protected void updateRefsAfterCreation() {
		final ORID rid = dataEntry.getIdentity();

		if (left != null) {
			if (left.dataEntry.setParent(rid))
				left.markDirty();
		}

		if (right != null) {
			if (right.dataEntry.setParent(rid))
				right.markDirty();
		}

		if (parent != null) {
			// XXX Sylvain: should be set when parent is saved
			// parentRid = parent.record.getIdentity();
			// if (dataEntry.setParent(parent.dataEntry.getIdentity()))
			// markDirty();
			if (parent.left == this) {
				if (parent.dataEntry.setLeft(rid))
					parent.markDirty();
			} else if (parent.right == this) {
				if (parent.dataEntry.setRight(rid))
					parent.markDirty();
			} else {
				OLogManager.instance().error(this, "[save]: Tree inconsitant entries.");
			}
		} else if (pTree.getRoot() == this) {
			if (pTree.dataTree.setRoot(rid))
				pTree.markDirty();
		}
	}

	/**
	 * Delete all the nodes recursively. IF they are not loaded in memory, load all the tree.
	 * 
	 * @throws IOException
	 */
	public OMVRBTreeEntryPersistent<K, V> delete() throws IOException {
		pTree.removeNodeFromMemory(this);
		pTree.removeEntry(dataEntry.getIdentity());

		// EARLY LOAD LEFT AND DELETE IT RECURSIVELY
		if (getLeft() != null)
			((OMVRBTreeEntryPersistent<K, V>) getLeft()).delete();

		// EARLY LOAD RIGHT AND DELETE IT RECURSIVELY
		if (getRight() != null)
			((OMVRBTreeEntryPersistent<K, V>) getRight()).delete();

		// DELETE MYSELF
		dataEntry.delete();
		clear();

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
		if (dataEntry == null)
			// DIRTY NODE, JUST REMOVE IT
			return 1;

		int totalDisconnected = 0;

		final ORID rid = dataEntry.getIdentity();

		if ((!dataEntry.isEntryDirty() || iForceDirty) && !pTree.isNodeEntryPoint(this)) {
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

	protected void clear() {
		// SPEED UP MEMORY CLAIM BY RESETTING INTERNAL FIELDS
		pTree = null;
		tree = null;
		dataEntry.clear();
		dataEntry = null;
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
		if (dataEntry == null)
			return null;

		if (parent == null && dataEntry.getParent().isValid()) {
			// System.out.println("Node " + record.getIdentity() + " is loading PARENT node " + parentRid + "...");

			// LAZY LOADING OF THE PARENT NODE
			parent = pTree.loadEntry(null, dataEntry.getParent());

			checkEntryStructure();

			if (parent != null) {
				// TRY TO ASSIGN IT FOLLOWING THE RID
				if (parent.dataEntry.getLeft().isValid() && parent.dataEntry.getLeft().equals(dataEntry.getIdentity()))
					parent.left = this;
				else if (parent.dataEntry.getRight().isValid() && parent.dataEntry.getRight().equals(dataEntry.getIdentity()))
					parent.right = this;
				else {
					OLogManager.instance().error(this, "getParent: Can't assign node %s to parent. Nodes parent-left=%s, parent-right=%s",
							dataEntry.getParent(), parent.dataEntry.getLeft(), parent.dataEntry.getRight());
				}
			}
		}
		return parent;
	}

	@Override
	public OMVRBTreeEntry<K, V> setParent(final OMVRBTreeEntry<K, V> iParent) {
		if (iParent != parent) {
			OMVRBTreeEntryPersistent<K, V> newParent = (OMVRBTreeEntryPersistent<K, V>) iParent;
			ORID newParentId = iParent == null ? ORecordId.EMPTY_RECORD_ID : newParent.dataEntry.getIdentity();

			parent = newParent;

			if (dataEntry.setParent(newParentId))
				markDirty();

			if (parent != null) {
				ORID thisRid = dataEntry.getIdentity();
				if (parent.left == this && !parent.dataEntry.getLeft().equals(thisRid))
					if (parent.dataEntry.setLeft(thisRid))
						parent.markDirty();
				if (parent.left != this && parent.dataEntry.getLeft().isValid() && parent.dataEntry.getLeft().equals(thisRid))
					parent.left = this;
				if (parent.right == this && !parent.dataEntry.getRight().equals(thisRid))
					if (parent.dataEntry.setRight(thisRid))
						parent.markDirty();
				if (parent.right != this && parent.dataEntry.getRight().isValid() && parent.dataEntry.getRight().equals(thisRid))
					parent.right = this;
			}
		}
		return iParent;
	}

	@Override
	public OMVRBTreeEntry<K, V> getLeft() {
		if (dataEntry == null)
			return null;
		if (left == null && dataEntry.getLeft().isValid()) {
			// LAZY LOADING OF THE LEFT LEAF
			left = pTree.loadEntry(this, dataEntry.getLeft());
			checkEntryStructure();
		}
		return left;
	}

	@Override
	public void setLeft(final OMVRBTreeEntry<K, V> iLeft) {
		if (iLeft != left) {
			OMVRBTreeEntryPersistent<K, V> newLeft = (OMVRBTreeEntryPersistent<K, V>) iLeft;
			ORID newLeftId = iLeft == null ? ORecordId.EMPTY_RECORD_ID : newLeft.dataEntry.getIdentity();

			left = newLeft;
			if (dataEntry.setLeft(newLeftId))
				markDirty();

			if (left != null && left.parent != this)
				left.setParent(this);

			checkEntryStructure();
		}
	}

	@Override
	public OMVRBTreeEntry<K, V> getRight() {
		if (dataEntry == null)
			return null;
		if (right == null && dataEntry.getRight().isValid()) {
			// LAZY LOADING OF THE RIGHT LEAF
			right = pTree.loadEntry(this, dataEntry.getRight());
			checkEntryStructure();
		}
		return right;
	}

	@Override
	public void setRight(final OMVRBTreeEntry<K, V> iRight) {
		if (iRight != right) {
			OMVRBTreeEntryPersistent<K, V> newRight = (OMVRBTreeEntryPersistent<K, V>) iRight;
			ORID newRightId = iRight == null ? ORecordId.EMPTY_RECORD_ID : newRight.dataEntry.getIdentity();

			right = newRight;
			if (dataEntry.setRight(newRightId))
				markDirty();

			if (right != null && right.parent != this)
				right.setParent(this);

			checkEntryStructure();
		}
	}

	public void checkEntryStructure() {
		if (!tree.isRuntimeCheckEnabled())
			return;

		if (dataEntry.getParent() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has parentRid null!\n", this);
		if (dataEntry.getLeft() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has leftRid null!\n", this);
		if (dataEntry.getRight() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has rightRid null!\n", this);

		if (this == left || dataEntry.getIdentity().isValid() && dataEntry.getIdentity().equals(dataEntry.getLeft()))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left that points to itself!\n", this);
		if (this == right || dataEntry.getIdentity().isValid() && dataEntry.getIdentity().equals(dataEntry.getRight()))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has right that points to itself!\n", this);
		if (left != null && left == right)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left and right equals!\n", this);

		if (left != null) {
			if (!left.dataEntry.getIdentity().equals(dataEntry.getLeft()))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong left node loaded: " + dataEntry.getLeft());
			if (left.parent != this)
				OLogManager.instance().error(this,
						"checkEntryStructure: Left node is not correctly connected to the parent" + dataEntry.getLeft());
		}

		if (right != null) {
			if (!right.dataEntry.getIdentity().equals(dataEntry.getRight()))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong right node loaded: " + dataEntry.getRight());
			if (right.parent != this)
				OLogManager.instance().error(this,
						"checkEntryStructure: Right node is not correctly connected to the parent" + dataEntry.getRight());
		}
	}

	@Override
	protected void copyFrom(final OMVRBTreeEntry<K, V> iSource) {
		if (dataEntry.copyFrom(((OMVRBTreeEntryPersistent<K, V>) iSource).dataEntry))
			markDirty();
	}

	@Override
	protected void insert(final int iIndex, final K iKey, final V iValue) {
		K oldKey = iIndex == 0 ? dataEntry.getKeyAt(0) : null;
		if (dataEntry.insertAt(iIndex, iKey, iValue))
			markDirty();

		if (iIndex == 0)
			pTree.updateEntryPoint(oldKey, this);
	}

	@Override
	protected void remove() {
		final int index = tree.getPageIndex();
		final K oldKey = index == 0 ? getKeyAt(0) : null;

		if (dataEntry.removeAt(index))
			markDirty();

		tree.setPageIndex(0);

		if (index == 0)
			pTree.updateEntryPoint(oldKey, this);
	}

	@Override
	public K getKeyAt(final int iIndex) {
		return dataEntry.getKeyAt(iIndex);
	}

	@Override
	protected V getValueAt(final int iIndex) {
		return dataEntry.getValueAt(iIndex);
	}

	/**
	 * Invalidate serialized Value associated in order to be re-marshalled on the next node storing.
	 */
	public V setValue(final V iValue) {
		V oldValue = getValue();

		int index = tree.getPageIndex();
		if (dataEntry.setValueAt(index, iValue))
			markDirty();

		return oldValue;
	}

	public int getSize() {
		return dataEntry != null ? dataEntry.getSize() : 0;
	}

	public int getPageSize() {
		return dataEntry.getPageSize();
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

	@Override
	public boolean getColor() {
		return dataEntry.getColor();
	}

	@Override
	protected void setColor(final boolean iColor) {
		if (dataEntry.setColor(iColor))
			markDirty();
	}

	protected void markDirty() {
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
		return dataEntry != null ? dataEntry.toString() : "entry cleared";
	}
}
