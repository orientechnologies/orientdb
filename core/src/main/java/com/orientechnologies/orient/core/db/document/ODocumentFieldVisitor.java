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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Is used in together with {@link
 * com.orientechnologies.orient.core.db.document.ODocumentFieldWalker} to visit all fields of
 * current document.
 */
public interface ODocumentFieldVisitor {
  /**
   * Visits currently processed field.
   *
   * @param type Filed type. May be null if absent in DB schema.
   * @param linkedType Linked type in case collection is processed. May be null if absent in DB
   *     schema.
   * @param value Field value.
   * @return New value of this field. If the same value is returned document content will not be
   *     changed.
   */
  Object visitField(OType type, OType linkedType, Object value);

  /**
   * Indicates whether we continue to visit document fields after current one or should stop fields
   * processing.
   *
   * @param type Filed type. May be null if absent in DB schema.
   * @param linkedType Linked type in case collection is processed. May be null if absent in DB
   *     schema.
   * @param value Field value.
   * @param newValue New value returned by {@link #visitField(OType, OType, Object)} method.
   * @return If false document processing will be stopped.
   */
  boolean goFurther(OType type, OType linkedType, Object value, Object newValue);

  /**
   * If currently processed value is collection or map of embedded documents or embedded document
   * itself then current method is called if it returns false then this collection will not be
   * visited.
   *
   * @param type Filed type. May be null if absent in DB schema.
   * @param linkedType Linked type in case collection is processed. May be null if absent in DB
   *     schema.
   * @param value Field value.
   * @return If false currently processed collection of embedded documents will not be visited.
   */
  boolean goDeeper(OType type, OType linkedType, Object value);

  /**
   * @return If false value returned by method {@link #visitField(OType, OType, Object)} will not be
   *     taken in account and field value will not be updated.
   */
  boolean updateMode();
}
