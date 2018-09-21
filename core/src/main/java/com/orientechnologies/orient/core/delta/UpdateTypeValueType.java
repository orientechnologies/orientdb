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
import com.orientechnologies.orient.core.record.impl.UpdateDeltaValueType;

/**
 * @author marko
 */
public class UpdateTypeValueType {

  private Object               value;
  private UpdateDeltaValueType updateType;
  private OTypeInterface       valueType;

  public UpdateTypeValueType() {

  }

  public UpdateTypeValueType(UpdateDeltaValueType updateType, Object value, OTypeInterface valueType) {
    this.value = value;
    this.updateType = updateType;
    this.valueType = valueType;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public void setUpdateType(UpdateDeltaValueType updateType) {
    this.updateType = updateType;
  }

  public void setValueType(OTypeInterface valueType) {
    this.valueType = valueType;
  }

  public Object getValue() {
    return value;
  }

  public UpdateDeltaValueType getUpdateType() {
    return updateType;
  }

  public OTypeInterface getValueType() {
    return valueType;
  }

}
