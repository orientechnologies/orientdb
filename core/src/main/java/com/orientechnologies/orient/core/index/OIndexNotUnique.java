/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
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
package com.orientechnologies.orient.core.index;

import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Index implementation that allows multiple values for the same key.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexNotUnique extends OIndexMultiValues {

  public OIndexNotUnique(String typeId, OIndexEngine<Set<OIdentifiable>> engine) {
    super(typeId, engine);
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return indexEngine.hasRangeQuerySupport();
  }
}
