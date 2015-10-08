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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Represents a binary field.
 * 
 * @author Luca Garulli
 */
public class OBinaryField {

  public final String         name;
  public final OType          type;
  public final BytesContainer bytes;

  public OBinaryField(final String iName, final OType iType, final BytesContainer iBytes) {
    name = iName;
    type = iType;
    bytes = iBytes;
  }

  public OBinaryField copy() {
    return new OBinaryField(name, type, bytes.copy());
  }
}
