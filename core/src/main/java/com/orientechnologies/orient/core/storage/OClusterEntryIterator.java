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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;
import java.util.Iterator;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.OClusterPosition;

public class OClusterEntryIterator implements Iterator<OPhysicalPosition> {
  private final OCluster         cluster;
  private OClusterPosition       current;
  private final OClusterPosition max;

  public OClusterEntryIterator(final OCluster iCluster) {
    cluster = iCluster;
    current = cluster.getFirstIdentity();
    max = cluster.getLastIdentity();
  }

  public OClusterEntryIterator(final OCluster iCluster, final OClusterPosition iBeginRange, final OClusterPosition iEndRange) throws IOException {
    cluster = iCluster;
    current = iBeginRange;
    max = OClusterPosition.INVALID_POSITION.compareTo(iEndRange) < 0 ? iEndRange : cluster.getLastIdentity();
  }

  public boolean hasNext() {
    return current!=null&&OClusterPosition.INVALID_POSITION.compareTo(max) < 0  && OClusterPosition.INVALID_POSITION.compareTo(current) < 0 && current.compareTo(max) <= 0;
  }

  public OPhysicalPosition next() {
    try {
      OPhysicalPosition physicalPosition = cluster.getPhysicalPosition(new OPhysicalPosition(current));
      if (current.compareTo(max) < 0){
        current = cluster.nextRecord(current);
      }else{
        current = null;
      }
      return physicalPosition;
    } catch (IOException e) {
      throw new ODatabaseException("Cannot read next record of cluster.", e);
    }
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
