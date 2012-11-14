/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.id;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 12.11.12
 */
public final class OClusterPositionLong extends OClusterPosition {
  private final long value;

  public OClusterPositionLong(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public OClusterPosition inc() {
    return new OClusterPositionLong(value + 1);
  }

  @Override
  public OClusterPosition dec() {
    return new OClusterPositionLong(value - 1);
  }

  @Override
  public boolean isValid() {
    return value != -1;
  }

  @Override
  public boolean isPersistent() {
    return value > -1;
  }

  @Override
  public boolean isNew() {
    return value < 0;
  }

  @Override
  public boolean isTemporary() {
    return value < -1;
  }

  @Override
  public byte[] toStream() {
    final byte[] content = new byte[OLongSerializer.LONG_SIZE];

    OLongSerializer.INSTANCE.serialize(value, content, 0);

    return content;
  }

  @Override
  public int compareTo(OClusterPosition otherPosition) {
    final OClusterPositionLong otherLongPosition = (OClusterPositionLong) otherPosition;

    if (value > otherLongPosition.value)
      return 1;
    else if (value < otherLongPosition.value)
      return -1;
    else
      return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OClusterPositionLong that = (OClusterPositionLong) o;

    if (value != that.value)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  @Override
  public int intValue() {
    return (int) value;
  }

  @Override
  public long longValue() {
    return value;
  }

  @Override
  public float floatValue() {
    return value;
  }

  @Override
  public double doubleValue() {
    return value;
  }
}
