/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OEmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OAllocatePositionOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateClusterOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ODeleteRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ORecycleRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OUpdateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OCreateHashTableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTablePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTableRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OCreateSBTreeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreeRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiRawOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiPutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiRemoveOperation;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public class OWALRecordsFactory {
  private final Map<Byte, Class> idToTypeMap = new HashMap<>();
  private final Map<Class, Byte> typeToIdMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  public byte[] toStream(OWriteableWALRecord walRecord, int contentSize) {
    byte[] content = new byte[contentSize];

    if (walRecord instanceof OUpdatePageRecord)
      content[0] = 0;
    else if (walRecord instanceof OFuzzyCheckpointStartRecord)
      content[0] = 1;
    else if (walRecord instanceof OFuzzyCheckpointEndRecord)
      content[0] = 2;
    else if (walRecord instanceof OFullCheckpointStartRecord)
      content[0] = 4;
    else if (walRecord instanceof OCheckpointEndRecord)
      content[0] = 5;
    else if (walRecord instanceof OAtomicUnitStartRecord)
      content[0] = 8;
    else if (walRecord instanceof OAtomicUnitEndRecord)
      content[0] = 9;
    else if (walRecord instanceof OFileCreatedWALRecord)
      content[0] = 10;
    else if (walRecord instanceof ONonTxOperationPerformedWALRecord)
      content[0] = 11;
    else if (walRecord instanceof OFileDeletedWALRecord)
      content[0] = 12;
    else if (walRecord instanceof OFileTruncatedWALRecord)
      content[0] = 13;
    else if (walRecord instanceof OEmptyWALRecord)
      content[0] = 14;
    else if (walRecord instanceof OAllocatePositionOperation)
      content[0] = 15;
    else if (walRecord instanceof OCreateRecordOperation)
      content[0] = 16;
    else if (walRecord instanceof ODeleteRecordOperation)
      content[0] = 17;
    else if (walRecord instanceof ORecycleRecordOperation)
      content[0] = 18;
    else if (walRecord instanceof OUpdateRecordOperation)
      content[0] = 19;
    else if (walRecord instanceof OHashTablePutOperation)
      content[0] = 20;
    else if (walRecord instanceof OHashTableRemoveOperation)
      content[0] = 21;
    else if (walRecord instanceof OSBTreePutOperation)
      content[0] = 22;
    else if (walRecord instanceof OSBTreeRemoveOperation)
      content[0] = 23;
    else if (walRecord instanceof OSBTreeBonsaiPutOperation)
      content[0] = 24;
    else if (walRecord instanceof OSBTreeBonsaiRemoveOperation)
      content[0] = 25;
    else if (walRecord instanceof OCreateClusterOperation)
      content[0] = 26;
    else if (walRecord instanceof OCreateHashTableOperation)
      content[0] = 27;
    else if (walRecord instanceof OCreateSBTreeOperation)
      content[0] = 28;
    else if (walRecord instanceof OCreateSBTreeBonsaiOperation)
      content[0] = 29;
    else if (walRecord instanceof OCreateSBTreeBonsaiRawOperation)
      content[0] = 30;
    else if (typeToIdMap.containsKey(walRecord.getClass())) {
      content[0] = typeToIdMap.get(walRecord.getClass());
    } else
      throw new IllegalArgumentException(walRecord.getClass().getName() + " class cannot be serialized.");

    walRecord.toStream(content, 1);

    return content;
  }

  public void toStream(OWriteableWALRecord walRecord, ByteBuffer buffer) {

    if (walRecord instanceof OUpdatePageRecord)
      buffer.put((byte) 0);
    else if (walRecord instanceof OFuzzyCheckpointStartRecord)
      buffer.put((byte) 1);
    else if (walRecord instanceof OFuzzyCheckpointEndRecord)
      buffer.put((byte) 2);
    else if (walRecord instanceof OFullCheckpointStartRecord)
      buffer.put((byte) 4);
    else if (walRecord instanceof OCheckpointEndRecord)
      buffer.put((byte) 5);
    else if (walRecord instanceof OAtomicUnitStartRecord)
      buffer.put((byte) 8);
    else if (walRecord instanceof OAtomicUnitEndRecord)
      buffer.put((byte) 9);
    else if (walRecord instanceof OFileCreatedWALRecord)
      buffer.put((byte) 10);
    else if (walRecord instanceof ONonTxOperationPerformedWALRecord)
      buffer.put((byte) 11);
    else if (walRecord instanceof OFileDeletedWALRecord)
      buffer.put((byte) 12);
    else if (walRecord instanceof OFileTruncatedWALRecord)
      buffer.put((byte) 13);
    else if (walRecord instanceof OEmptyWALRecord)
      buffer.put((byte) 14);
    else if (walRecord instanceof OAllocatePositionOperation)
      buffer.put((byte) 15);
    else if (walRecord instanceof OCreateRecordOperation)
      buffer.put((byte) 16);
    else if (walRecord instanceof ODeleteRecordOperation)
      buffer.put((byte) 17);
    else if (walRecord instanceof ORecycleRecordOperation)
      buffer.put((byte) 18);
    else if (walRecord instanceof OUpdateRecordOperation)
      buffer.put((byte) 19);
    else if (walRecord instanceof OHashTablePutOperation)
      buffer.put((byte) 20);
    else if (walRecord instanceof OHashTableRemoveOperation)
      buffer.put((byte) 21);
    else if (walRecord instanceof OSBTreePutOperation)
      buffer.put((byte) 22);
    else if (walRecord instanceof OSBTreeRemoveOperation)
      buffer.put((byte) 23);
    else if (walRecord instanceof OSBTreeBonsaiPutOperation)
      buffer.put((byte) 24);
    else if (walRecord instanceof OSBTreeBonsaiRemoveOperation)
      buffer.put((byte) 25);
    else if (walRecord instanceof OCreateClusterOperation)
      buffer.put((byte) 26);
    else if (walRecord instanceof OCreateHashTableOperation)
      buffer.put((byte) 27);
    else if (walRecord instanceof OCreateSBTreeOperation)
      buffer.put((byte) 28);
    else if (walRecord instanceof OCreateSBTreeBonsaiOperation)
      buffer.put((byte) 29);
    else if (walRecord instanceof OCreateSBTreeBonsaiRawOperation)
      buffer.put((byte) 30);
    else if (typeToIdMap.containsKey(walRecord.getClass())) {
      buffer.put(typeToIdMap.get(walRecord.getClass()));
    } else
      throw new IllegalArgumentException(walRecord.getClass().getName() + " class cannot be serialized.");

    walRecord.toStream(buffer);
  }

  public OWriteableWALRecord fromStream(byte[] content) {
    OWriteableWALRecord walRecord;
    switch (content[0]) {
    case 0:
      walRecord = new OUpdatePageRecord();
      break;
    case 1:
      walRecord = new OFuzzyCheckpointStartRecord();
      break;
    case 2:
      walRecord = new OFuzzyCheckpointEndRecord();
      break;
    case 4:
      walRecord = new OFullCheckpointStartRecord();
      break;
    case 5:
      walRecord = new OCheckpointEndRecord();
      break;
    case 8:
      walRecord = new OAtomicUnitStartRecord();
      break;
    case 9:
      walRecord = new OAtomicUnitEndRecord();
      break;
    case 10:
      walRecord = new OFileCreatedWALRecord();
      break;
    case 11:
      walRecord = new ONonTxOperationPerformedWALRecord();
      break;
    case 12:
      walRecord = new OFileDeletedWALRecord();
      break;
    case 13:
      walRecord = new OFileTruncatedWALRecord();
      break;
    case 14:
      walRecord = new OEmptyWALRecord();
      break;
    case 15:
      walRecord = new OAllocatePositionOperation();
      break;
    case 16:
      walRecord = new OCreateRecordOperation();
      break;
    case 17:
      walRecord = new ODeleteRecordOperation();
      break;
    case 18:
      walRecord = new ORecycleRecordOperation();
      break;
    case 19:
      walRecord = new OUpdateRecordOperation();
      break;
    case 20:
      walRecord = new OHashTablePutOperation();
      break;
    case 21:
      walRecord = new OHashTableRemoveOperation();
      break;
    case 22:
      walRecord = new OSBTreePutOperation();
      break;
    case 23:
      walRecord = new OSBTreeRemoveOperation();
      break;
    case 24:
      walRecord = new OSBTreeBonsaiPutOperation();
      break;
    case 25:
      walRecord = new OSBTreeBonsaiRemoveOperation();
      break;
    case 26:
      walRecord = new OCreateClusterOperation();
      break;
    case 27:
      walRecord = new OCreateHashTableOperation();
      break;
    case 28:
      walRecord = new OCreateSBTreeOperation();
      break;
    case 29:
      walRecord = new OCreateSBTreeBonsaiOperation();
      break;
    case 30:
      walRecord = new OCreateSBTreeBonsaiRawOperation();
      break;
    default:
      if (idToTypeMap.containsKey(content[0]))
        try {
          walRecord = (OWriteableWALRecord) idToTypeMap.get(content[0]).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new IllegalStateException("Cannot deserialize passed in record", e);
        }
      else
        throw new IllegalStateException("Cannot deserialize passed in wal record.");
    }

    walRecord.fromStream(content, 1);

    return walRecord;
  }

  public int serializedSize(OWriteableWALRecord walRecord) {
    return walRecord.serializedSize() + 1;
  }

  public void registerNewRecord(byte id, Class<? extends OWriteableWALRecord> type) {
    typeToIdMap.put(type, id);
    idToTypeMap.put(id, type);
  }
}
