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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OClusterEntryIterator implements Iterator<OPhysicalPosition> {
  private final OCluster cluster;

  private final long min;

  private OPhysicalPosition[] positionsToProcess;
  private int positionsIndex;

  public OClusterEntryIterator(final OCluster iCluster) {
    cluster = iCluster;
    try {
      min = cluster.getFirstPosition();
    } catch (IOException ioe) {
      throw new IllegalStateException("Exception during iterator creation", ioe);
    }

    positionsToProcess = null;
    positionsIndex = -1;
  }

  @Override
  public boolean hasNext() {
    if (min == ORID.CLUSTER_POS_INVALID) return false;

    if (positionsToProcess == null) return true;

    return positionsToProcess.length != 0;
  }

  @Override
  public OPhysicalPosition next() {
    try {
      if (positionsIndex == -1) {
        positionsToProcess = cluster.ceilingPositions(new OPhysicalPosition(min));
        positionsIndex = 0;
      }

      if (positionsToProcess.length == 0) throw new NoSuchElementException();

      final OPhysicalPosition result = positionsToProcess[positionsIndex];
      positionsIndex++;

      if (positionsIndex >= positionsToProcess.length) {
        positionsToProcess =
            cluster.higherPositions(positionsToProcess[positionsToProcess.length - 1]);
        positionsIndex = 0;
      }

      return result;
    } catch (IOException e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot read next record of cluster"), e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
