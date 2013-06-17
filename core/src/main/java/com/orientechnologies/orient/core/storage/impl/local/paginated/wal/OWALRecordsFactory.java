/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OUpdatePageRecord;

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class OWALRecordsFactory {
  private Map<Byte, Class>               idToTypeMap = new HashMap<Byte, Class>();
  private Map<Class, Byte>               typeToIdMap = new HashMap<Class, Byte>();

  public static final OWALRecordsFactory INSTANCE    = new OWALRecordsFactory();

  public byte[] toStream(OWALRecord walRecord) {
    int contentSize = walRecord.serializedSize() + 1;
    byte[] content = new byte[contentSize];

    if (walRecord instanceof OUpdatePageRecord)
      content[0] = 0;
    else if (walRecord instanceof OFuzzyCheckpointStartRecord)
      content[0] = 1;
    else if (walRecord instanceof OFuzzyCheckpointEndRecord)
      content[0] = 2;
    else if (walRecord instanceof ODirtyPagesRecord)
      content[0] = 3;
    else if (walRecord instanceof OFullCheckpointStartRecord)
      content[0] = 4;
    else if (walRecord instanceof OCheckpointEndRecord)
      content[0] = 5;
    else if (walRecord instanceof OAddNewPageRecord)
      content[0] = 6;
    else if (walRecord instanceof OClusterStateRecord)
      content[0] = 7;
    else if (walRecord instanceof OAtomicUnitStartRecord)
      content[0] = 8;
    else if (walRecord instanceof OAtomicUnitEndRecord)
      content[0] = 9;
    else if (walRecord instanceof OFreePageChangeRecord)
      content[0] = 10;
    else if (typeToIdMap.containsKey(walRecord.getClass())) {
      content[0] = typeToIdMap.get(walRecord.getClass());
    } else
      throw new IllegalArgumentException(walRecord.getClass().getName() + " class can not be serialized.");

    walRecord.toStream(content, 1);

    return content;
  }

  public OWALRecord fromStream(byte[] content) {
    OWALRecord walRecord;
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
    case 3:
      walRecord = new ODirtyPagesRecord();
      break;
    case 4:
      walRecord = new OFullCheckpointStartRecord();
      break;
    case 5:
      walRecord = new OCheckpointEndRecord();
      break;
    case 6:
      walRecord = new OAddNewPageRecord();
      break;
    case 7:
      walRecord = new OClusterStateRecord();
      break;
    case 8:
      walRecord = new OAtomicUnitStartRecord();
      break;
    case 9:
      walRecord = new OAtomicUnitEndRecord();
      break;
    case 10:
      walRecord = new OFreePageChangeRecord();
      break;
    default:
      if (idToTypeMap.containsKey(content[0]))
        try {
          walRecord = (OWALRecord) idToTypeMap.get(content[0]).newInstance();
        } catch (InstantiationException e) {
          throw new IllegalStateException("Can not deserialize passed in record", e);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("Can not deserialize passed in record", e);
        }
      else
        throw new IllegalStateException("Can not deserialize passed in wal record.");
    }

    walRecord.fromStream(content, 1);

    return walRecord;
  }

  public void registerNewRecord(byte id, Class<? extends OWALRecord> type) {
    typeToIdMap.put(type, id);
    idToTypeMap.put(id, type);
  }
}
