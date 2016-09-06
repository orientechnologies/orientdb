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

package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * {@link OObjectFieldHandlingStrategy} that delegates to the default {@link ODocument#field(String)} implementation.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class OObjectSimpleFieldHandlingStrategy implements OObjectFieldHandlingStrategy {

  @Override
  public ODocument store(ODocument iRecord, String fieldName, Object fieldValue, OType suggestedFieldType) {

    return iRecord.field(fieldName, fieldValue, deriveFieldType(iRecord, fieldName, suggestedFieldType));
  }

  @Override
  public Object load(ODocument iRecord, String fieldName, OType suggestedFieldType) {

    OType fieldType = deriveFieldType(iRecord, fieldName, suggestedFieldType);

    /*
     * For backward compatibility reasons this approach is kept (though it's kind of odd): asking a field value using a suggested
     * type which does not match the actual type of the field implies doing a cast and updating the type of the field in the
     * document.
     */
    if (iRecord.fieldType(fieldName) != fieldType) {
      iRecord.field(fieldName, iRecord.field(fieldName), fieldType);
    }

    return iRecord.field(fieldName);
  }

  /**
   * Derives the type of a field in a document.
   * 
   * @param iRecord
   * @param fieldName
   * @param requestedFieldType
   * @return derived field type
   */
  protected OType deriveFieldType(ODocument iRecord, String fieldName, OType requestedFieldType) {

    // Schema defined types can not be ignored
    if (iRecord.getSchemaClass().existsProperty(fieldName)) {
      return iRecord.getSchemaClass().getProperty(fieldName).getType();
    }

    // New type
    if (requestedFieldType != null) {
      return requestedFieldType;
    }

    // Existing type (not fixed by the schema)
    return iRecord.fieldType(fieldName);
  }
}
