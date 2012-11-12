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
package com.orientechnologies.orient.core.id;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Version of ORID which is used in auto sharded configuration only.
 * 
 * @author Andrey Lomakin
 * @since 30.10.12
 */
public class OAutoShardedRecordId extends ORecordId {
  public long mostSigDHTClusterId;
  public long leastSigDHTClusterId;

  public OAutoShardedRecordId() {
  }

  public OAutoShardedRecordId(int iClusterId, long iPosition, long mostSigDHTClusterId, long leastSigDHTClusterId) {
    super(iClusterId, iPosition);
    this.mostSigDHTClusterId = mostSigDHTClusterId;
    this.leastSigDHTClusterId = leastSigDHTClusterId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OAutoShardedRecordId that = (OAutoShardedRecordId) o;

    if (leastSigDHTClusterId != that.leastSigDHTClusterId)
      return false;
    if (mostSigDHTClusterId != that.mostSigDHTClusterId)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (mostSigDHTClusterId ^ (mostSigDHTClusterId >>> 32));
    result = 31 * result + (int) (leastSigDHTClusterId ^ (leastSigDHTClusterId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(12);
    buffer.append(PREFIX);
    buffer.append(clusterId);
    buffer.append(SEPARATOR);
    buffer.append(String.format("%1$016x", clusterPosition));
    buffer.append(String.format("%1$016x", mostSigDHTClusterId));
    buffer.append(String.format("%1$016x", leastSigDHTClusterId));

    return buffer.toString();
  }

  @Override
  public int compareTo(OIdentifiable iOther) {
    if (iOther == this)
      return 0;

    if (iOther == null)
      return 1;

    final OAutoShardedRecordId otherRid = (OAutoShardedRecordId) iOther.getIdentity();

    final int otherClusterId = otherRid.getClusterId();
    if (clusterId > otherClusterId)
      return 1;
    else if (clusterId < otherClusterId)
      return -1;

    int result = compareUnsignedLongs(getClusterPosition(), otherRid.getClusterPosition());
    if (result != 0)
      return result;

    result = compareUnsignedLongs(mostSigDHTClusterId, otherRid.mostSigDHTClusterId);
    if (result != 0)
      return result;

    result = compareUnsignedLongs(leastSigDHTClusterId, otherRid.leastSigDHTClusterId);

    return result;
  }

  private static int compareUnsignedLongs(long longOne, long longTwo) {
    if (longOne == longTwo)
      return 0;

    return lessThanUnsigned(longOne, longTwo) ? -1 : 1;
  }

  private static boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }
}
