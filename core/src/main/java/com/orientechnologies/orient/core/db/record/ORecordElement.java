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
package com.orientechnologies.orient.core.db.record;

/**
 * Base interface that represents a record element.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ORecordElement {
  /** Available record statuses. */
  enum STATUS {
    NOT_LOADED,
    LOADED,
    MARSHALLING,
    UNMARSHALLING
  }

  /**
   * Marks the instance as dirty. The dirty status could be propagated up if the implementation
   * supports ownership concept.
   *
   * @return The object it self. Useful to call methods in chain.
   */
  <RET> RET setDirty();

  void setDirtyNoChanged();

  /** @return Returns record element which contains given one. */
  ORecordElement getOwner();
}
