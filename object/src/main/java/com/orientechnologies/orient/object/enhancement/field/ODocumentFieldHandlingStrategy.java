/*
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

package com.orientechnologies.orient.object.enhancement.field;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Strategy handling how to store and retrieve data in documents.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public interface ODocumentFieldHandlingStrategy {

  /**
   * Stores an object in a document
   * 
   * @param iRecord
   * @param fieldName
   * @param fieldValue
   * @param suggestedFieldType
   *          ignored if the type is set in the schema
   */
  ODocument store(ODocument iRecord, String fieldName, Object fieldValue, OType suggestedFieldType);

  /**
   * Retrieves a field from a document
   * 
   * @param iRecord
   * @param fieldName
   * @param suggestedFieldType
   *          ignored if the type is set in the schema
   * @return field value
   */
  Object load(ODocument iRecord, String fieldName, OType suggestedFieldType);

}