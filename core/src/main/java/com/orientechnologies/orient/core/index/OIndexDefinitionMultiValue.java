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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import java.util.Map;

/**
 * Interface that indicates that index definition is based on collection of values but not on single
 * value.
 *
 * @author <a href="mailto:a.lomakin@orientechnologies.com">Andrey Lomakin</a>
 * @since 20.12.11
 */
public interface OIndexDefinitionMultiValue extends OIndexDefinition {

  /**
   * Converts passed in value in the key of single index entry.
   *
   * @param param Value to convert.
   * @return Index key.
   */
  public Object createSingleValue(final Object... param);

  /**
   * Process event that contains operation on collection and extract values that should be added
   * removed from index to reflect collection changes in the given index.
   *
   * @param changeEvent Event that describes operation that was performed on collection.
   * @param keysToAdd Values that should be added to related index.
   * @param keysToRemove Values that should be removed to related index.
   */
  public void processChangeEvent(
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove);
}
