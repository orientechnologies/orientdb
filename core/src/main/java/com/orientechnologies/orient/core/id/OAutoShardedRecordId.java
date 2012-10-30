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
  public int compareTo(OIdentifiable iOther) {
    int result = super.compareTo(iOther);
    if (result != 0)
      return result;
    final OAutoShardedRecordId otherRid = (OAutoShardedRecordId) iOther.getIdentity();

    if (mostSigDHTClusterId > otherRid.mostSigDHTClusterId)
      return 1;
    else if (mostSigDHTClusterId < otherRid.mostSigDHTClusterId)
      return -1;

    if (leastSigDHTClusterId > otherRid.leastSigDHTClusterId)
      return 1;
    else if (leastSigDHTClusterId < otherRid.leastSigDHTClusterId)
      return -1;

    return 0;
  }
}
