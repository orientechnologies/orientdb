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

package com.orientechnologies.orient.core.serialization;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface for objects which are hold inside of document as field values and can serialize
 * yourself into document. In such way it is possible to serialize complex types and do not break
 * compatibility with non-Java binary drivers.
 *
 * <p>After serialization into document additional field {@link #CLASS_NAME} will be added. This
 * field contains value of class of original object.
 *
 * <p>During deserialization of embedded object if embedded document contains {@link #CLASS_NAME}
 * field we try to find class with given name and only if this class implements {@link
 * ODocumentSerializable} interface it will be converted to the object. So it is pretty safe to use
 * field with {@link #CLASS_NAME} in ordinary documents if it is needed.
 *
 * <p>Class which implements this interface should have public no-arguments constructor.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/27/14
 */
public interface ODocumentSerializable {
  String CLASS_NAME = "__orientdb_serilized_class__ ";

  ODocument toDocument();

  void fromDocument(ODocument document);
}
