package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
class OLinearHashingBucket<K extends OClusterPosition, V extends OPhysicalPosition> {
  public static final int BUCKET_MAX_SIZE = 64;
  K[]                     keys            = (K[]) new OClusterPosition[BUCKET_MAX_SIZE];
  V[]                     values          = (V[]) new OPhysicalPosition[BUCKET_MAX_SIZE];
  int                     size;

  public V get(final K key) {
    for (int i = 0; i < size; i++) {
      if (key.equals(keys[i])) {
        return values[i];
      }
    }
    return null;
  }

  public List<V> getLargestRecords(final byte signature) {
    List<V> result = new ArrayList<V>(size / 10);
    for (int i = 0; i < size;) {
      if (OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(keys[i]) == signature) {
        result.add(values[i]);
        --size;
        keys[i] = keys[size];
        values[i] = values[size];
        keys[size] = null;
        values[size] = null;
      } else {
        ++i;
      }
    }

    assert !result.isEmpty();

    return result;
  }

  public Collection<V> getContent() {
    return Arrays.asList(values).subList(0, size);
  }

  public void emptyBucket() {
    size = 0;
  }

  public int deleteKey(OClusterPosition key) {
    for (int i = 0; i < size; i++) {
      if (key.equals(keys[i])) {
        keys[i] = keys[size - 1];
        values[i] = values[size - 1];
        size--;
        return i;
      }
    }
    return -1;
  }

  public List<V> getSmallestRecords(int maxSizeOfRecordsArray) {
    byte signature = 127;
    List<V> result = new ArrayList<V>(size / 10);
    for (int i = 0; i < size; ++i) {
      byte keySignature = OLinearHashingHashCalculatorFactory.INSTANCE.calculateSignature(keys[i]);
      if (keySignature < signature) {
        signature = keySignature;
        result.clear();
        result.add(values[i]);
      } else if (keySignature == signature) {
        result.add(values[i]);
      }
    }

    assert !result.isEmpty();

    if (result.size() > maxSizeOfRecordsArray) {
      return new ArrayList<V>();
    } else {
      return result;
    }
  }

  public void add(List<V> smallestRecords) {
    if (smallestRecords.size() > (BUCKET_MAX_SIZE - size)) {
      throw new IllegalArgumentException("array size should be less than existing free space in bucket");
    } else {
      for (V smallestRecord : smallestRecords) {
        keys[size] = (K) smallestRecord.clusterPosition;
        values[size] = smallestRecord;
        size++;
      }
    }
  }
}
