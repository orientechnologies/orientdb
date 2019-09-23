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

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/2/13
 */
public class OCheckpointEndRecord extends OAbstractWALRecord {
  public OCheckpointEndRecord() {
  }

  @Override
  public int toStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
  }

  @Override
  public int fromStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public int getId() {
    return WALRecordTypes.CHECKPOINT_END_RECORD;
  }
}
