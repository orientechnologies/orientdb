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
package com.orientechnologies.common.types;

public class OModifiableLong extends Number implements Comparable<OModifiableLong> {
  public long value;

  public OModifiableLong() {
    value = 0;
  }

  public OModifiableLong(final long iValue) {
    value = iValue;
  }

  public void setValue(final long iValue) {
    value = iValue;
  }

  public long getValue() {
    return value;
  }

  public void increment() {
    value++;
  }

  public void increment(final long iValue) {
    value += iValue;
  }

  public void decrement() {
    value--;
  }

  public void decrement(final long iValue) {
    value -= iValue;
  }

  public int compareTo(final OModifiableLong anotherInteger) {
    long thisVal = value;
    long anotherVal = anotherInteger.value;

    return (thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0 : 1);
  }

  @Override
  public byte byteValue() {
    return (byte) value;
  }

  @Override
  public short shortValue() {
    return (short) value;
  }

  @Override
  public float floatValue() {
    return value;
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public int intValue() {
    return (int) value;
  }

  @Override
  public long longValue() {
    return value;
  }

  public Long toLong() {
    return Long.valueOf(this.value);
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof OModifiableLong) {
      return value == ((OModifiableLong) o).value;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(value).hashCode();
  }

  @Override
  public String toString() {
    return String.valueOf(this.value);
  }
}
