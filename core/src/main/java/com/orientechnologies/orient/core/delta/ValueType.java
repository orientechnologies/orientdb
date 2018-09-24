/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.delta;

import com.orientechnologies.orient.core.metadata.schema.OTypeInterface;

/**
 * @author mdjurovi
 */
public class ValueType {
  private Object         value;
  private OTypeInterface type;

  private ValueType() {

  }

  public ValueType(Object value, OTypeInterface type) {
    this.value = value;
    this.type = type;
  }

  public <T> T getValue() {
    return (T) value;
  }

  public OTypeInterface getType() {
    return type;
  }
}
