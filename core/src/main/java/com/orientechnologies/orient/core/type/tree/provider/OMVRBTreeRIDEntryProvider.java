/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.type.tree.provider;

import java.io.IOException;
import java.util.Arrays;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Handles a set of references optimizing the memory space used. This is used by LINKSET type and as SET of links of Not Unique
 * indexes. The binary chunk is allocated the first time and never changes. This takes more memory but assure zero fragmentation at
 * the storage level. Tree size field is used only in the root node to avoid to store the parent document.<br>
 * Structure of binary chunk:<br>
 * <code>
 * +-----------+-----------+--------+------------+----------+-----------+---------------------+<br>
 * | TREE SIZE | NODE SIZE | COLOR .| PARENT RID | LEFT RID | RIGHT RID | RID LIST .......... |<br>
 * +-----------+-----------+--------+------------+----------+-----------+---------------------+<br>
 * | 4 bytes . | 4 bytes . | 1 byte | 10 bytes ..| 10 bytes | 10 bytes .| 10 * MAX_SIZE bytes |<br>
 * +-----------+-----------+--------+------------+----------+-----------+---------------------+<br>
 * = 39 bytes + 10 * PAGE-SIZE bytes<br>
 * Where:
 * <ul>
 * <li><b>TREE SIZE</b> as signed integer (4 bytes) containing the size of the tree. Only the root node has this value updated, so to know the size of the collection you need to load the root node and get this field. other nodes can contain not updated values because upon rotation of pieces of the tree (made during tree rebalancing) the root can change and the old root will have the "old" size as dirty.</li>
 * <li><b>NODE SIZE</b> as signed integer (4 bytes) containing number of entries in this node. It's always <= to the page-size defined at the tree level and equals for all the nodes. By default page-size is 16 items</li>
 * <li><b>COLOR</b> as 1 byte containing 1=Black, 0=Red. To know more about the meaning of this look at [http://en.wikipedia.org/wiki/Red%E2%80%93black_tree Red-Black Trees]</li>
 * <li><b>PARENT RID</b> as [Concepts#RecordID RID] (10 bytes) of the parent node record</li>
 * <li><b>LEFT RID</b> as [Concepts#RecordID RID] (10 bytes) of the left node record</li>
 * <li><b>RIGHT RID</b> as [Concepts#RecordID RID] (10 bytes) of the right node record</li>
 * <li><b>RID LIST</b> as the list of [Concepts#RecordID RIDs] containing the references to the records. This is pre-allocated to the configured page-size. Since each [Concepts#RecordID RID] takes 10 bytes, a page-size of 16 means 16 x 10bytes = 160bytes</li>
 * </ul>
 * The size of the tree-node on disk (and memory) is fixed to avoid fragmentation. To compute it: 39 bytes + 10 * PAGE-SIZE bytes. For a page-size = 16 you'll have 39 + 160 = 199 bytes.
 * </code>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com) *
 * 
 */
public class OMVRBTreeRIDEntryProvider extends OMVRBTreeEntryDataProviderAbstract<OIdentifiable, OIdentifiable> {
  private static final long     serialVersionUID = 1L;

  protected final static int    OFFSET_TREESIZE  = 0;
  protected final static int    OFFSET_NODESIZE  = OFFSET_TREESIZE + OBinaryProtocol.SIZE_INT;
  protected final static int    OFFSET_COLOR     = OFFSET_NODESIZE + OBinaryProtocol.SIZE_INT;
  protected final static int    OFFSET_PARENT    = OFFSET_COLOR + OBinaryProtocol.SIZE_BYTE;
  protected final static int    OFFSET_LEFT      = OFFSET_PARENT + ORecordId.PERSISTENT_SIZE;
  protected final static int    OFFSET_RIGHT     = OFFSET_LEFT + ORecordId.PERSISTENT_SIZE;
  protected final static int    OFFSET_RIDLIST   = OFFSET_RIGHT + ORecordId.PERSISTENT_SIZE;

  private int                   treeSize;
  private final OIdentifiable[] rids;

  public OMVRBTreeRIDEntryProvider(final OMVRBTreeRIDProvider iTreeDataProvider) {
    super(iTreeDataProvider, OFFSET_RIDLIST + (iTreeDataProvider.getDefaultPageSize() * ORecordId.PERSISTENT_SIZE));
    rids = OGlobalConfiguration.MVRBTREE_RID_NODE_SAVE_MEMORY.getValueAsBoolean() ? null : new OIdentifiable[pageSize];
  }

  public OMVRBTreeRIDEntryProvider(final OMVRBTreeRIDProvider iTreeDataProvider, final ORID iRID) {
    super(iTreeDataProvider, iRID);
    pageSize = treeDataProvider.getDefaultPageSize();
    rids = OGlobalConfiguration.MVRBTREE_RID_NODE_SAVE_MEMORY.getValueAsBoolean() ? null : new OIdentifiable[pageSize];
  }

  /**
   * Lazy unmarshall the RID if not in memory.
   */
  public OIdentifiable getKeyAt(final int iIndex) {
    if (rids != null && rids[iIndex] != null)
      return rids[iIndex];

    final ORecordId rid = itemFromStream(iIndex);

    if (rids != null)
      rids[iIndex] = rid;

    return rid;
  }

  /**
   * Returns the key
   */
  public OIdentifiable getValueAt(final int iIndex) {
    return getKeyAt(iIndex);
  }

  public boolean setValueAt(int iIndex, final OIdentifiable iValue) {
    if (iValue == null)
      return false;

    try {
      itemToStream(iValue, iIndex);
    } catch (IOException e) {
      throw new OSerializationException("Cannot serialize entryRID object: " + this, e);
    }

    if (rids != null)
      rids[iIndex] = iValue;

    return setDirty();
  }

  public boolean insertAt(final int iIndex, final OIdentifiable iKey, final OIdentifiable iValue) {
    if (iIndex < size) {
      // MOVE RIGHT TO MAKE ROOM FOR THE ITEM
      stream.move(getKeyPositionInStream(iIndex), ORecordId.PERSISTENT_SIZE);
      if (rids != null)
        System.arraycopy(rids, iIndex, rids, iIndex + 1, size - iIndex - 1);
    }

    try {
      itemToStream(iKey, iIndex);
    } catch (IOException e) {
      throw new OSerializationException("Cannot serialize entryRID object: " + this, e);
    }

    if (rids != null)
      rids[iIndex] = iKey;

    size++;

    return setDirty();
  }

  public boolean removeAt(final int iIndex) {
    if (iIndex > -1 && iIndex < size - 1) {
      // SHIFT LEFT THE VALUES
      stream.move(getKeyPositionInStream(iIndex + 1), ORecordId.PERSISTENT_SIZE * -1);
      if (rids != null)
        System.arraycopy(rids, iIndex + 1, rids, iIndex, size - iIndex - 1);
    }

    size--;

    // FREE RESOURCES
    if (rids != null)
      rids[size] = null;

    return setDirty();
  }

  public boolean copyDataFrom(final OMVRBTreeEntryDataProvider<OIdentifiable, OIdentifiable> iFrom, final int iStartPosition) {
    size = iFrom.getSize() - iStartPosition;
    final OMVRBTreeRIDEntryProvider from = (OMVRBTreeRIDEntryProvider) iFrom;
    moveToIndex(0).copyFrom(from.moveToIndex(iStartPosition), size * ORecordId.PERSISTENT_SIZE);
    if (rids != null)
      System.arraycopy(from.rids, iStartPosition, rids, 0, size);
    return setDirty();
  }

  public boolean truncate(final int iNewSize) {
    moveToIndex(iNewSize).fill((size - iNewSize) * ORecordId.PERSISTENT_SIZE, (byte) 0);
    if (rids != null)
      Arrays.fill(rids, iNewSize, size, null);
    size = iNewSize;
    return setDirty();
  }

  public boolean copyFrom(final OMVRBTreeEntryDataProvider<OIdentifiable, OIdentifiable> iSource) {
    final OMVRBTreeRIDEntryProvider source = (OMVRBTreeRIDEntryProvider) iSource;

    stream = source.stream;
    size = source.size;

    return setDirty();
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    if (stream == null)
      stream = new OMemoryStream(iStream);
    else
      stream.setSource(iStream);

    treeSize = stream.jump(OFFSET_TREESIZE).getAsInteger();
    size = stream.jump(OFFSET_NODESIZE).getAsInteger();
    color = stream.jump(OFFSET_COLOR).getAsBoolean();
    parentRid.fromStream(stream.jump(OFFSET_PARENT));
    leftRid.fromStream(stream.jump(OFFSET_LEFT));
    rightRid.fromStream(stream.jump(OFFSET_RIGHT));

    if (rids != null)
      // CREATE IN MEMORY RIDS FROM STREAM
      Arrays.fill(rids, null);

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    if (stream == null)
      stream = new OMemoryStream();

    try {
      stream.jump(OFFSET_TREESIZE).set(treeSize);
      stream.jump(OFFSET_NODESIZE).set(size);
      stream.jump(OFFSET_COLOR).set(color);
      parentRid.toStream(stream.jump(OFFSET_PARENT));
      leftRid.toStream(stream.jump(OFFSET_LEFT));
      rightRid.toStream(stream.jump(OFFSET_RIGHT));

      if (rids != null)
        // STREAM RIDS
        for (int i = 0; i < size; ++i)
          if (rids[i] != null)
            itemToStream(rids[i], i);

    } catch (IOException e) {
      throw new OSerializationException("Cannot serialize tree entry RID node: " + this, e);
    }

    // RETURN DIRECTLY THE UNDERLYING BUFFER SINCE IT'S FIXED
    final byte[] buffer = stream.getInternalBuffer();
    record.fromStream(buffer);
    return buffer;
  }

  protected OMemoryStream moveToIndex(final int iIndex) {
    return stream.jump(getKeyPositionInStream(iIndex));
  }

  protected int getKeyPositionInStream(final int iIndex) {
    return OFFSET_RIDLIST + (iIndex * ORecordId.PERSISTENT_SIZE);
  }

  public int getTreeSize() {
    return treeSize;
  }

  public boolean setTreeSize(final int treeSize) {
    if (this.treeSize != treeSize) {
      this.treeSize = treeSize;
      setDirty();
      return true;
    }
    return false;
  }

  protected ORecordId itemFromStream(final int iIndex) {
    return new ORecordId().fromStream(moveToIndex(iIndex));
  }

  protected int itemToStream(final OIdentifiable iKey, final int iIndex) throws IOException {
    return iKey.getIdentity().toStream(moveToIndex(iIndex));
  }
}
