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

/**
 * @author Andrey Lomakin
 * @since 25.04.13
 */
public class OWALRecordsFactory {
  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  public byte[] toStream(OWALRecord walRecord) {
    int contentSize = walRecord.serializedSize() + 1;
    byte[] content = new byte[contentSize];

    if (walRecord instanceof OSetPageDataRecord)
      content[0] = 0;
    else if (walRecord instanceof OStartAtomicPageUpdateRecord)
      content[0] = 1;
    else if (walRecord instanceof OEndAtomicPageUpdateRecord)
      content[0] = 2;
    else if (walRecord instanceof OFuzzyCheckpointStartRecord)
      content[0] = 3;
    else if (walRecord instanceof OFuzzyCheckpointEndRecord)
      content[0] = 4;
    else if (walRecord instanceof ODirtyPagesRecord)
      content[0] = 5;
    else if (walRecord instanceof OCheckpointStartRecord)
      content[0] = 6;
    else if (walRecord instanceof OCheckpointEndRecord)
      content[0] = 7;
    else if (walRecord instanceof OAddNewPageRecord)
      content[0] = 8;
    else
      throw new IllegalArgumentException(walRecord.getClass().getName() + " class can not be serialized.");

    walRecord.toStream(content, 1);

    return content;
  }

  public OWALRecord fromStream(byte[] content) {
    OWALRecord walRecord;
    switch (content[0]) {
    case 0:
      walRecord = new OSetPageDataRecord();
      break;
    case 1:
      walRecord = new OStartAtomicPageUpdateRecord();
      break;
    case 2:
      walRecord = new OEndAtomicPageUpdateRecord();
      break;
    case 3:
      walRecord = new OFuzzyCheckpointStartRecord();
      break;
    case 4:
      walRecord = new OFuzzyCheckpointEndRecord();
      break;
    case 5:
      walRecord = new ODirtyPagesRecord();
      break;
    case 6:
      walRecord = new OCheckpointStartRecord();
      break;
    case 7:
      walRecord = new OCheckpointEndRecord();
      break;
    case 8:
      walRecord = new OAddNewPageRecord();
      break;
    default:
      throw new IllegalStateException("Can not deserialize passed in wal record.");
    }

    walRecord.fromStream(content, 1);

    return walRecord;
  }
}
