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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Map;

/**
 * Base class for all multivalue index definitions that contains base functionality which can be
 * reused by concrete implementations.
 */
public abstract class OAbstractIndexDefinitionMultiValue extends OPropertyIndexDefinition
    implements OIndexDefinitionMultiValue {
  protected OAbstractIndexDefinitionMultiValue() {}

  protected OAbstractIndexDefinitionMultiValue(
      final String iClassName, final String iField, final OType iType) {
    super(iClassName, iField, iType);
  }

  protected void processAdd(
      final Object value,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    if (value == null) return;

    final Integer removeCount = keysToRemove.get(value);
    if (removeCount != null) {
      int newRemoveCount = removeCount - 1;
      if (newRemoveCount > 0) keysToRemove.put(value, newRemoveCount);
      else keysToRemove.remove(value);
    } else {
      final Integer addCount = keysToAdd.get(value);
      if (addCount != null) keysToAdd.put(value, addCount + 1);
      else keysToAdd.put(value, 1);
    }
  }

  protected void processRemoval(
      final Object value,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    if (value == null) return;

    final Integer addCount = keysToAdd.get(value);
    if (addCount != null) {
      int newAddCount = addCount - 1;
      if (newAddCount > 0) keysToAdd.put(value, newAddCount);
      else keysToAdd.remove(value);
    } else {
      final Integer removeCount = keysToRemove.get(value);
      if (removeCount != null) keysToRemove.put(value, removeCount + 1);
      else keysToRemove.put(value, 1);
    }
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }
}
