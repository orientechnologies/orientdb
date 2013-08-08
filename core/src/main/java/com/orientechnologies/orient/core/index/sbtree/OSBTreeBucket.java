package com.orientechnologies.orient.core.index.sbtree;

import java.util.Comparator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTreeBucket<K, V> {
  private static final int            MAGIC_NUMBER_OFFSET    = 0;
  private static final int            CRC32_OFFSET           = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            WAL_SEGMENT_OFFSET     = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            WAL_POSITION_OFFSET    = WAL_SEGMENT_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int            FREE_POINTER_OFFSET    = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int            SIZE_OFFSET            = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int            POSITIONS_ARRAY_OFFSET = FREE_POINTER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int             MAX_BUCKET_SIZE_BYTES  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final long                  cachePointer;

  private final OBinarySerializer<K>  keySerializer;
  private final OBinarySerializer<V>  valueSerializer;

  private final Comparator<? super K> comparator             = ODefaultComparator.INSTANCE;

  public OSBTreeBucket(long cachePointer, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    this.cachePointer = cachePointer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public boolean isEmpty() {
    return false; // To change body of created methods use File | Settings | File Templates.
  }

  public int find(K key) {
    return 0; // To change body of created methods use File | Settings | File Templates.
  }

  public void remove(int entryIndex) {
    // To change body of created methods use File | Settings | File Templates.
  }

  public boolean put(int index, K key, V value) {
    return false; // To change body of created methods use File | Settings | File Templates.
  }

  public int size() {
    return 0; // To change body of created methods use File | Settings | File Templates.
  }

  public static final class SBTreeEntry<K, V> {
    public final long leftChild;
    public final long rightChild;
    public final K    key;
    public final V    value;

    public SBTreeEntry(long leftChild, long rightChild, K key, V value) {
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SBTreeEntry that = (SBTreeEntry) o;

      if (leftChild != that.leftChild)
        return false;
      if (rightChild != that.rightChild)
        return false;
      if (!key.equals(that.key))
        return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (leftChild ^ (leftChild >>> 32));
      result = 31 * result + (int) (rightChild ^ (rightChild >>> 32));
      result = 31 * result + key.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "SBTreeEntry{" + "leftChild=" + leftChild + ", rightChild=" + rightChild + ", key=" + key + ", value=" + value + '}';
    }
  }
}
