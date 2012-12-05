package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPosition;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com Date: 8/28/12 Time: 10:34 AM
 */
class OLinearHashingIndex {
  private List<OLinearHashingIndexElement> indexContent = new ArrayList<OLinearHashingIndexElement>(2);

  public void addNewPosition() {
    indexContent.add(new OLinearHashingIndexElement());
  }

  public void addNewPosition(int position) {
    indexContent.add(position, new OLinearHashingIndexElement());
  }

  public int getChainDisplacement(int numberOfChain) {
    return indexContent.get(numberOfChain).displacement & 0xFF;
  }

  public byte getChainSignature(int numberOfChain) {
    return indexContent.get(numberOfChain).signature;
  }

  public int incrementChainDisplacement(int chainNumber, int bucketSize) {
    if ((indexContent.get(chainNumber).displacement & 0xFF) == 254 && bucketSize == OLinearHashingBucket.BUCKET_MAX_SIZE) {
      indexContent.get(chainNumber).displacement = (byte) 253;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 255) {
      indexContent.get(chainNumber).displacement = (byte) 254;
    }

    return indexContent.get(chainNumber).displacement & 0xFF;
  }

  public int decrementDisplacement(int chainNumber, int bucketSize, boolean nextIndexWasRemoved) {
    if ((indexContent.get(chainNumber).displacement & 0xFF) < 253 && nextIndexWasRemoved) {
      indexContent.get(chainNumber).displacement = (byte) 253;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 253 && bucketSize < OLinearHashingBucket.BUCKET_MAX_SIZE) {
      indexContent.get(chainNumber).displacement = (byte) 254;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 254 && bucketSize == 0) {
      indexContent.get(chainNumber).displacement = (byte) 255;
    }

    return indexContent.get(chainNumber).displacement & 0xFF;
  }

  public int decrementDisplacement(int chainNumber, int bucketSize) {
    return decrementDisplacement(chainNumber, bucketSize, false);
  }

  public void updateSignature(int chainNumber, OClusterPosition[] keys, int size) {
    byte signature = Byte.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      if (signature < OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(keys[i])) {
        signature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(keys[i]);
      }
    }
    indexContent.get(chainNumber).signature = signature;
  }

  public void updateSignature(int bucketNumber, byte signature) {
    indexContent.get(bucketNumber).signature = signature;
  }

  public void updateDisplacement(int chainNumber, byte displacement) {
    indexContent.get(chainNumber).displacement = displacement;
  }

  public void remove(int index) {
    indexContent.remove(index);
  }

  public int bucketCount() {
    return indexContent.size();
  }

  public void clearChainInfo(int chainNumber) {
    indexContent.get(chainNumber).displacement = (byte) 255;
    indexContent.get(chainNumber).signature = Byte.MAX_VALUE;
  }

  public void moveRecord(int oldPositionInIndex, int newPositionInIndex) {
    OLinearHashingIndexElement indexElement = indexContent.get(oldPositionInIndex);
    indexContent.remove(indexElement);
    indexContent.add(newPositionInIndex, indexElement);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0, indexContentSize = indexContent.size(); i < indexContentSize; i++) {
      OLinearHashingIndexElement indexElement = indexContent.get(i);
      builder.append("|\t\t").append(i).append("\t\t|\t\t").append(indexElement.displacement & 0xFF).append("\t\t|\t\t")
          .append(indexElement.signature & 0xFF).append("\t\t|\n");
    }
    return builder.toString();
  }

  public void clear() {
    indexContent.clear();
  }
}
