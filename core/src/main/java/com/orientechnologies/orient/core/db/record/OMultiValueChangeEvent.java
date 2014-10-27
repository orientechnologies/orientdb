/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.db.record;

/**
 * Event that contains information about operation that is performed on tracked collection.
 * 
 * @param <K>
 *          Value that indicates position of item inside collection.
 * @param <V>
 *          Item value.
 */
public class OMultiValueChangeEvent<K, V> {
  /**
   * Operation that is performed on collection.
   */
  public static enum OChangeType {
    ADD, UPDATE, REMOVE
  }

  /**
   * Operation that is performed on collection.
   */
  private final OChangeType changeType;

  /**
   * Value that indicates position of item inside collection.
   */
  private final K           key;

  /**
   * New item value.
   */
  private V                 value;

  /**
   * Previous item value.
   */
  private V                 oldValue;

  private boolean           changesOwnerContent = true;

  public OMultiValueChangeEvent(OChangeType changeType, K key, V value) {
    this.changeType = changeType;
    this.key = key;
    this.value = value;
  }

  public OMultiValueChangeEvent(OChangeType changeType, K key, V value, V oldValue) {
    this.changeType = changeType;
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
  }

  public OMultiValueChangeEvent(OChangeType changeType, K key, V value, V oldValue, boolean changesOwnerContent) {
    this.changeType = changeType;
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.changesOwnerContent = changesOwnerContent;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public OChangeType getChangeType() {
    return changeType;
  }

  public V getOldValue() {
    return oldValue;
  }

  public boolean isChangesOwnerContent() {
    return changesOwnerContent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OMultiValueChangeEvent that = (OMultiValueChangeEvent) o;

    return changesOwnerContent == that.changesOwnerContent && changeType == that.changeType
        && !(key != null ? !key.equals(that.key) : that.key != null)
        && !(oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null)
        && !(value != null ? !value.equals(that.value) : that.value != null);

  }

  @Override
  public int hashCode() {
    int result = changeType != null ? changeType.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
    result = 31 * result + (changesOwnerContent ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OMultiValueChangeEvent{" + "changeType=" + changeType + ", key=" + key + ", value=" + value + ", oldValue=" + oldValue
        + '}';
  }
}
