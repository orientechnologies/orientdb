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
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeEntryDataProvider;

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
	protected OMVRBTreeEntryDataProvider<K, V>	dataProvider;
	protected OMVRBTreePersistent<K, V>					pTree;

	protected OMVRBTreeEntryPersistent<K, V>		parent;
	protected OMVRBTreeEntryPersistent<K, V>		left;
	protected OMVRBTreeEntryPersistent<K, V>		right;

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
		dataProvider = pTree.dataProvider.getEntry(iRecordId);
		init();
		parent = iParent;
		// setParent(iParent);
		pTree.addNodeInMemory(this);
	}

	/**
	 * Make a new cell with given key, value, and parent, and with <tt>null</tt> child links, and BLACK color.
	 */
	public OMVRBTreeEntryPersistent(final OMVRBTreePersistent<K, V> iTree, final K iKey, final V iValue,
			final OMVRBTreeEntryPersistent<K, V> iParent) {
		super(iTree);
		pTree = iTree;
		dataProvider = pTree.dataProvider.createEntry();
		dataProvider.insertAt(0, iKey, iValue);
		init();
		setParent(iParent);
		pTree.addNodeInMemory(this);
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
		dataProvider = pTree.dataProvider.createEntry();
		dataProvider.copyDataFrom(pParent.dataProvider, iPosition);
		if (pParent.dataProvider.truncate(iPosition))
			pParent.markDirty();
		init();
		setParent(pParent);
		pTree.addNodeInMemory(this);
		// created entry : force dispatch dirty node.
		markDirty();
	}

	public OMVRBTreeEntryDataProvider<K, V> getProvider() {
		return dataProvider;
	}

	/**
	 * Assures that all the links versus parent, left and right are consistent.
	 * 
	 */
	public OMVRBTreeEntryPersistent<K, V> save() throws OSerializationException {
		if (!dataProvider.isEntryDirty())
			return this;

		final boolean isNew = dataProvider.getIdentity().isNew();

		// FOR EACH NEW LINK, SAVE BEFORE
		if (left != null && left.dataProvider.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				left.dataProvider.save();
				left.updateRefsAfterCreation();
			} else
				left.save();
		}
		if (right != null && right.dataProvider.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				right.dataProvider.save();
				right.updateRefsAfterCreation();
			} else
				right.save();
		}
		if (parent != null && parent.dataProvider.getIdentity().isNew()) {
			if (isNew) {
				// TEMPORARY INCORRECT SAVE FOR GETTING AN ID. WILL BE SET DIRTY AGAIN JUST AFTER
				parent.dataProvider.save();
				parent.updateRefsAfterCreation();
			} else
				parent.save();
		}

		dataProvider.save();

		// RE-ASSIGN RID
		if (isNew)
			updateRefsAfterCreation();

		// if (parent != null)
		// if (!parent.record.getIdentity().equals(parentRid))
		// OLogManager.instance().error(this,
		// "[save]: Tree node %s has parentRid '%s' different by the rid of the assigned parent node: %s", record.getIdentity(),
		// parentRid, parent.record.getIdentity());

		checkEntryStructure();

		if (pTree.searchNodeInCache(dataProvider.getIdentity()) != this) {
			// UPDATE THE CACHE
			pTree.addNodeInMemory(this);
		}

		return this;
	}

	protected void updateRefsAfterCreation() {
		final ORID rid = dataProvider.getIdentity();

		if (left != null) {
			if (left.dataProvider.setParent(rid))
				left.markDirty();
		}

		if (right != null) {
			if (right.dataProvider.setParent(rid))
				right.markDirty();
		}

		if (parent != null) {
			if (parent.left == this) {
				if (parent.dataProvider.setLeft(rid))
					parent.markDirty();
			} else if (parent.right == this) {
				if (parent.dataProvider.setRight(rid))
					parent.markDirty();
			} else {
				OLogManager.instance().error(this, "[save]: Tree inconsistent entries.");
			}
		} else if (pTree.getRoot() == this) {
			if (pTree.dataProvider.setRoot(rid))
				pTree.markDirty();
		}
	}

	/**
	 * Delete all the nodes recursively. IF they are not loaded in memory, load all the tree.
	 * 
	 * @throws IOException
	 */
	public OMVRBTreeEntryPersistent<K, V> delete() throws IOException {
		if (dataProvider != null) {
			pTree.removeNodeFromMemory(this);
			pTree.removeEntry(dataProvider.getIdentity());

			// EARLY LOAD LEFT AND DELETE IT RECURSIVELY
			if (getLeft() != null)
				((OMVRBTreeEntryPersistent<K, V>) getLeft()).delete();

			// EARLY LOAD RIGHT AND DELETE IT RECURSIVELY
			if (getRight() != null)
				((OMVRBTreeEntryPersistent<K, V>) getRight()).delete();

			// DELETE MYSELF
			dataProvider.delete();
			clear();
		}

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
		if (dataProvider == null)
			// DIRTY NODE, JUST REMOVE IT
			return 1;

		int totalDisconnected = 0;

		final ORID rid = dataProvider.getIdentity();

		if ((!dataProvider.isEntryDirty() || iForceDirty) && !pTree.isNodeEntryPoint(this)) {
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
						"Node " + rid + " has the right (" + right + ") unlinked to itself. It links to " + right.parent);

			totalDisconnected += right.disconnect(iForceDirty, iLevel + 1);
			right = null;
		}

		return totalDisconnected;
	}

	protected void clear() {
		// SPEED UP MEMORY CLAIM BY RESETTING INTERNAL FIELDS
		pTree = null;
		tree = null;
		dataProvider.clear();
		dataProvider = null;
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
		if (dataProvider == null)
			return null;

		if (parent == null && dataProvider.getParent().isValid()) {
			// System.out.println("Node " + record.getIdentity() + " is loading PARENT node " + parentRid + "...");

			// LAZY LOADING OF THE PARENT NODE
			parent = pTree.loadEntry(null, dataProvider.getParent());

			checkEntryStructure();

			if (parent != null) {
				// TRY TO ASSIGN IT FOLLOWING THE RID
				if (parent.dataProvider.getLeft().isValid() && parent.dataProvider.getLeft().equals(dataProvider.getIdentity()))
					parent.left = this;
				else if (parent.dataProvider.getRight().isValid() && parent.dataProvider.getRight().equals(dataProvider.getIdentity()))
					parent.right = this;
				else {
					OLogManager.instance().error(this, "getParent: Cannot assign node %s to parent. Nodes parent-left=%s, parent-right=%s",
							dataProvider.getParent(), parent.dataProvider.getLeft(), parent.dataProvider.getRight());
				}
			}
		}
		return parent;
	}

	@Override
	public OMVRBTreeEntry<K, V> setParent(final OMVRBTreeEntry<K, V> iParent) {
		if (iParent != parent) {
			OMVRBTreeEntryPersistent<K, V> newParent = (OMVRBTreeEntryPersistent<K, V>) iParent;
			ORID newParentId = iParent == null ? ORecordId.EMPTY_RECORD_ID : newParent.dataProvider.getIdentity();

			parent = newParent;

			if (dataProvider.setParent(newParentId))
				markDirty();

			if (parent != null) {
				ORID thisRid = dataProvider.getIdentity();
				if (parent.left == this && !parent.dataProvider.getLeft().equals(thisRid))
					if (parent.dataProvider.setLeft(thisRid))
						parent.markDirty();
				if (parent.left != this && parent.dataProvider.getLeft().isValid() && parent.dataProvider.getLeft().equals(thisRid))
					parent.left = this;
				if (parent.right == this && !parent.dataProvider.getRight().equals(thisRid))
					if (parent.dataProvider.setRight(thisRid))
						parent.markDirty();
				if (parent.right != this && parent.dataProvider.getRight().isValid() && parent.dataProvider.getRight().equals(thisRid))
					parent.right = this;
			}
		}
		return iParent;
	}

	@Override
	public OMVRBTreeEntry<K, V> getLeft() {
		if (dataProvider == null)
			return null;
		if (left == null && dataProvider.getLeft().isValid()) {
			// LAZY LOADING OF THE LEFT LEAF
			left = pTree.loadEntry(this, dataProvider.getLeft());
			checkEntryStructure();
		}
		return left;
	}

	@Override
	public void setLeft(final OMVRBTreeEntry<K, V> iLeft) {
		if (iLeft != left) {
			OMVRBTreeEntryPersistent<K, V> newLeft = (OMVRBTreeEntryPersistent<K, V>) iLeft;
			ORID newLeftId = iLeft == null ? ORecordId.EMPTY_RECORD_ID : newLeft.dataProvider.getIdentity();

			left = newLeft;
			if (dataProvider.setLeft(newLeftId))
				markDirty();

			if (left != null && left.parent != this)
				left.setParent(this);

			checkEntryStructure();
		}
	}

	@Override
	public OMVRBTreeEntry<K, V> getRight() {
		if (dataProvider == null)
			return null;
		if (right == null && dataProvider.getRight().isValid()) {
			// LAZY LOADING OF THE RIGHT LEAF
			right = pTree.loadEntry(this, dataProvider.getRight());
			checkEntryStructure();
		}
		return right;
	}

	@Override
	public void setRight(final OMVRBTreeEntry<K, V> iRight) {
		if (iRight != right) {
			OMVRBTreeEntryPersistent<K, V> newRight = (OMVRBTreeEntryPersistent<K, V>) iRight;
			ORID newRightId = iRight == null ? ORecordId.EMPTY_RECORD_ID : newRight.dataProvider.getIdentity();

			right = newRight;
			if (dataProvider.setRight(newRightId))
				markDirty();

			if (right != null && right.parent != this)
				right.setParent(this);

			checkEntryStructure();
		}
	}

	public void checkEntryStructure() {
		if (!tree.isRuntimeCheckEnabled())
			return;

		if (dataProvider.getParent() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has parentRid null!\n", this);
		if (dataProvider.getLeft() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has leftRid null!\n", this);
		if (dataProvider.getRight() == null)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has rightRid null!\n", this);

		if (this == left || dataProvider.getIdentity().isValid() && dataProvider.getIdentity().equals(dataProvider.getLeft()))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left that points to itself!\n", this);
		if (this == right || dataProvider.getIdentity().isValid() && dataProvider.getIdentity().equals(dataProvider.getRight()))
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has right that points to itself!\n", this);
		if (left != null && left == right)
			OLogManager.instance().error(this, "checkEntryStructure: Node %s has left and right equals!\n", this);

		if (left != null) {
			if (!left.dataProvider.getIdentity().equals(dataProvider.getLeft()))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong left node loaded: " + dataProvider.getLeft());
			if (left.parent != this)
				OLogManager.instance().error(this,
						"checkEntryStructure: Left node is not correctly connected to the parent" + dataProvider.getLeft());
		}

		if (right != null) {
			if (!right.dataProvider.getIdentity().equals(dataProvider.getRight()))
				OLogManager.instance().error(this, "checkEntryStructure: Wrong right node loaded: " + dataProvider.getRight());
			if (right.parent != this)
				OLogManager.instance().error(this,
						"checkEntryStructure: Right node is not correctly connected to the parent" + dataProvider.getRight());
		}
	}

	@Override
	protected void copyFrom(final OMVRBTreeEntry<K, V> iSource) {
		if (dataProvider.copyFrom(((OMVRBTreeEntryPersistent<K, V>) iSource).dataProvider))
			markDirty();
	}

	@Override
	protected void insert(final int iIndex, final K iKey, final V iValue) {
		K oldKey = iIndex == 0 ? dataProvider.getKeyAt(0) : null;
		if (dataProvider.insertAt(iIndex, iKey, iValue))
			markDirty();

		if (iIndex == 0)
			pTree.updateEntryPoint(oldKey, this);
	}

	@Override
	protected void remove() {
		final int index = tree.getPageIndex();
		final K oldKey = index == 0 ? getKeyAt(0) : null;

		if (dataProvider.removeAt(index))
			markDirty();

		tree.setPageIndex(index - 1);

		if (index == 0)
			pTree.updateEntryPoint(oldKey, this);
	}

	@Override
	public K getKeyAt(final int iIndex) {
		return dataProvider.getKeyAt(iIndex);
	}

	@Override
	protected V getValueAt(final int iIndex) {
		return dataProvider.getValueAt(iIndex);
	}

	/**
	 * Invalidate serialized Value associated in order to be re-marshalled on the next node storing.
	 */
	public V setValue(final V iValue) {
		V oldValue = getValue();

		int index = tree.getPageIndex();
		if (dataProvider.setValueAt(index, iValue))
			markDirty();

		return oldValue;
	}

	public int getSize() {
		return dataProvider != null ? dataProvider.getSize() : 0;
	}

	public int getPageSize() {
		return dataProvider.getPageSize();
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
		return dataProvider.getColor();
	}

	@Override
	protected void setColor(final boolean iColor) {
		if (dataProvider.setColor(iColor))
			markDirty();
	}

	public void markDirty() {
		pTree.signalNodeChanged(this);
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
		return dataProvider != null ? dataProvider.toString() : "entry cleared";
	}
}
