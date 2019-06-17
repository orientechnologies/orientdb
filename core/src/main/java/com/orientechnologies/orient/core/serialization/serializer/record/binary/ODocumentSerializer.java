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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public interface ODocumentSerializer {

  void serialize(ODocument document, BytesContainer bytes, boolean iClassOnly);

  void serializeWithClassName(ODocument document, BytesContainer bytes, boolean iClassOnly);

  int serializeValue(BytesContainer bytes, Object value, OType type, OType linkedType);

  void deserialize(ODocument document, BytesContainer bytes);

  void deserializeWithClassName(ODocument document, BytesContainer bytes);

  void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields);

  Object deserializeValue(BytesContainer bytes, OType type, ODocument ownerDocument);

  OBinaryField deserializeField(BytesContainer bytes, OClass iClass, String iFieldName);

  OBinaryField deserializeFieldWithClassName(BytesContainer bytes, OClass iClass, String iFieldName);

  OBinaryComparator getComparator();

  /**
   * Returns the array of field names with no values.
   *
   * @param reference TODO
   */
  String[] getFieldNames(ODocument reference, BytesContainer iBytes, boolean readClassName);

  boolean isSerializingClassNameByDefault();

  boolean isSerializingClassNameForEmbedded();

  <RET> RET deserializeFieldTyped(BytesContainer record, String iFieldName, boolean isEmbedded, int serializerVersion);

  HelperClasses.Tuple<Integer, OType> getPointerAndTypeFromCurrentPosition(BytesContainer bytes);

  void deserializeDebug(BytesContainer bytes, ODatabaseDocumentInternal db, ORecordSerializationDebug debugInfo,
      OImmutableSchema schema);
}
