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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * {@link OObjectFieldHandlingStrategy} that deals with fields (depending on their type) in a smarter way than a
 * {@link OObjectSimpleFieldHandlingStrategy}.
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class OObjectSmartFieldHandlingStrategy extends OObjectSimpleFieldHandlingStrategy {

  private final Map<OType, OObjectFieldOTypeHandlingStrategy> customTypeHandlers = new HashMap<OType, OObjectFieldOTypeHandlingStrategy>();

  /**
   * Constructor
   * 
   * @param typeHandlers
   */
  public OObjectSmartFieldHandlingStrategy(Map<OType, OObjectFieldOTypeHandlingStrategy> typeHandlers) {
    this.customTypeHandlers.putAll(typeHandlers);

    // Validate the strategy mappings
    OObjectFieldOTypeHandlingStrategy currentStrategy;
    for (OType oType : this.customTypeHandlers.keySet()) {
      currentStrategy = this.customTypeHandlers.get(oType);
      if (!oType.equals(currentStrategy.getOType())) {
        throw new IllegalArgumentException("Strategy " + currentStrategy.getClass() + " can not handle fields with type: " + oType);
      }
    }
  }

  @Override
  public ODocument store(ODocument iRecord, String fieldName, Object fieldValue, OType suggestedFieldType) {

    OType fieldType = deriveFieldType(iRecord, fieldName, suggestedFieldType);

    if (fieldType == null) {
      return super.store(iRecord, fieldName, fieldValue, suggestedFieldType);
    }

    if (this.customTypeHandlers.containsKey(fieldType)) {
      return this.customTypeHandlers.get(fieldType).store(iRecord, fieldName, fieldValue);
    }

    return super.store(iRecord, fieldName, fieldValue, suggestedFieldType);
  }

  @Override
  public Object load(ODocument iRecord, String fieldName, OType suggestedFieldType) {

    OType fieldType = deriveFieldType(iRecord, fieldName, suggestedFieldType);

    if (this.customTypeHandlers.containsKey(fieldType)) {
      return this.customTypeHandlers.get(fieldType).load(iRecord, fieldName);
    }

    return super.load(iRecord, fieldName, suggestedFieldType);
  }
}
