package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.Iterator;

/** Created by luigidellaquila on 25/10/16. */
public class ORidSetIterator implements Iterator<ORID> {

  private final Iterator<ORID> negativesIterator;
  private ORidSet set;
  private int currentCluster = -1;
  private long currentId = -1;

  protected ORidSetIterator(ORidSet set) {
    this.set = set;
    this.negativesIterator = set.negatives.iterator();
    fetchNext();
  }

  @Override
  public boolean hasNext() {
    return negativesIterator.hasNext() || currentCluster >= 0;
  }

  @Override
  public ORID next() {
    if (negativesIterator.hasNext()) {
      return negativesIterator.next();
    }
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    ORecordId result = new ORecordId(currentCluster, currentId);
    currentId++;
    fetchNext();
    return result;
  }

  private void fetchNext() {
    if (currentCluster < 0) {
      currentCluster = 0;
      currentId = 0;
    }

    long currentArrayPos = currentId / 63;
    long currentBit = currentId % 63;
    int block = (int) (currentArrayPos / set.maxArraySize);
    int blockPositionByteInt = (int) (currentArrayPos % set.maxArraySize);

    while (currentCluster < set.content.length) {
      while (set.content[currentCluster] != null && block < set.content[currentCluster].length) {
        while (set.content[currentCluster][block] != null
            && blockPositionByteInt < set.content[currentCluster][block].length) {
          if (currentBit == 0 && set.content[currentCluster][block][blockPositionByteInt] == 0L) {
            blockPositionByteInt++;
            currentArrayPos++;
            continue;
          }
          if (set.contains(new ORecordId(currentCluster, currentArrayPos * 63 + currentBit))) {
            currentId = currentArrayPos * 63 + currentBit;
            return;
          } else {
            currentBit++;
            if (currentBit > 63) {
              currentBit = 0;
              blockPositionByteInt++;
              currentArrayPos++;
            }
          }
        }
        if (set.content[currentCluster][block] == null
            && set.content[currentCluster].length >= block) {
          currentArrayPos += set.maxArraySize;
        }
        block++;
        blockPositionByteInt = 0;
        currentBit = 0;
      }
      block = 0;
      currentBit = 0;
      currentArrayPos = 0;
      blockPositionByteInt = 0;
      currentCluster++;
    }

    currentCluster = -1;
  }
}
