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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Represents a binary field.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OBinaryField {

  public final String name;
  public final OType type;
  public final BytesContainer bytes;
  public final OCollate collate;

  public OBinaryField(
      final String iName, final OType iType, final BytesContainer iBytes, final OCollate iCollate) {
    name = iName;
    type = iType;
    bytes = iBytes;
    collate = iCollate;
  }

  public OBinaryField copy() {
    return new OBinaryField(name, type, bytes.copy(), collate);
  }
}
