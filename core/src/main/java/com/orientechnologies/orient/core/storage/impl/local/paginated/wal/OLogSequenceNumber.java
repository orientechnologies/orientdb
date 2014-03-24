/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public class OLogSequenceNumber implements Comparable<OLogSequenceNumber> {
  private final long segment;
  private final long position;

  public OLogSequenceNumber(long segment, long position) {
    this.segment = segment;
    this.position = position;
  }

  public long getSegment() {
    return segment;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OLogSequenceNumber that = (OLogSequenceNumber) o;

    if (position != that.position)
      return false;
    if (segment != that.segment)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (segment ^ (segment >>> 32));
    result = 31 * result + (int) (position ^ (position >>> 32));
    return result;
  }

  @Override
  public int compareTo(OLogSequenceNumber otherNumber) {
    if (segment > otherNumber.segment)
      return 1;
    if (segment < otherNumber.segment)
      return -1;

    if (position > otherNumber.position)
      return 1;
    else if (position < otherNumber.position)
      return -1;

    return 0;
  }

  @Override
  public String toString() {
    return "OLogSequenceNumber{" + "segment=" + segment + ", position=" + position + '}';
  }
}
