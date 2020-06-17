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
package com.orientechnologies.orient.core.metadata.sequence;

/** @author mdjurovi */
public enum SequenceOrderType {
  ORDER_POSITIVE((byte) 1),
  ORDER_NEGATIVE((byte) 2);

  private byte val;

  private SequenceOrderType(byte val) {
    this.val = val;
  }

  public byte getValue() {
    return val;
  }

  public static SequenceOrderType fromValue(byte val) {
    switch (val) {
      case (byte) 1:
        return ORDER_POSITIVE;
      case (byte) 2:
        return ORDER_NEGATIVE;
      default:
        return ORDER_POSITIVE;
    }
  }
}
