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
package com.orientechnologies.orient.core.record.impl;

/**
 * @author mdjurovi
 */
public enum UpdateDeltaValueType {
  UPDATE, LIST_UPDATE, LIST_ELEMENT_ADD, LIST_ELEMENT_REMOVE, LIST_ELEMENT_UPDATE, LIST_ELEMENT_CHANGE, CHANGE, RIDBAG_UPDATE, UNKNOWN;

  public byte getOrd() {
    switch (this) {
    case UPDATE:
      return 1;
    case LIST_UPDATE:
      return 2;
    case LIST_ELEMENT_ADD:
      return 3;
    case LIST_ELEMENT_REMOVE:
      return 4;
    case LIST_ELEMENT_UPDATE:
      return 5;
    case LIST_ELEMENT_CHANGE:
      return 6;
    case CHANGE:
      return 7;
    case RIDBAG_UPDATE:
      return 8;
    default:
      return 0;
    }
  }

  public static UpdateDeltaValueType fromOrd(Byte ordValue) {
    if (ordValue == null) {
      return UNKNOWN;
    }
    switch (ordValue) {
    case 1:
      return UPDATE;
    case 2:
      return LIST_UPDATE;
    case 3:
      return LIST_ELEMENT_ADD;
    case 4:
      return LIST_ELEMENT_REMOVE;
    case 5:
      return LIST_ELEMENT_UPDATE;
    case 6:
      return LIST_ELEMENT_CHANGE;
    case 7:
      return CHANGE;
    case 8:
      return RIDBAG_UPDATE;
    case 0:
    default:
      return UNKNOWN;
    }
  }

  public boolean isListElementOperation() {
    return this == LIST_ELEMENT_ADD || this == LIST_ELEMENT_CHANGE || this == LIST_ELEMENT_REMOVE || this == LIST_ELEMENT_UPDATE;
  }
}
