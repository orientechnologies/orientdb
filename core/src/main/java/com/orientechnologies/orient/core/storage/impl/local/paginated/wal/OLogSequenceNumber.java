/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Immutable number representing the position in WAL file (LSN).
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 29.04.13
 */
public class OLogSequenceNumber implements Comparable<OLogSequenceNumber> {
  private final long segment;
  private final long position;

  public OLogSequenceNumber(final long segment, final long position) {
    this.segment = segment;
    this.position = position;
  }

  public OLogSequenceNumber(final DataInput in) throws IOException {
    this.segment = in.readLong();
    this.position = in.readLong();
  }

  public long getSegment() {
    return segment;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final OLogSequenceNumber that = (OLogSequenceNumber) o;

    if (position != that.position) return false;
    if (segment != that.segment) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (segment ^ (segment >>> 32));
    result = 31 * result + (int) (position ^ (position >>> 32));
    return result;
  }

  @Override
  public int compareTo(final OLogSequenceNumber otherNumber) {
    if (segment > otherNumber.segment) return 1;
    if (segment < otherNumber.segment) return -1;

    if (position > otherNumber.position) return 1;
    else if (position < otherNumber.position) return -1;

    return 0;
  }

  public void toStream(final DataOutput out) throws IOException {
    out.writeLong(segment);
    out.writeLong(position);
  }

  @Override
  public String toString() {
    return "OLogSequenceNumber{segment=" + segment + ", position=" + position + '}';
  }
}
