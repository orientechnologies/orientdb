package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Special implementation of Java Set&lt;ORID&gt; to efficiently handle memory and performance. It
 * does not store actual RIDs, but it only keeps track that a RID was stored, so the iterator will
 * return new instances.
 *
 * @author Luigi Dell'Aquila
 */
public class ORidSet implements Set<ORID> {

  protected static int INITIAL_BLOCK_SIZE = 4096;

  /*
   * cluster / offset / bitmask
   * eg. inserting #12:0 you will have content[12][0][0] = 1
   * eg. inserting #12:(63*maxArraySize + 1) you will have content[12][1][0] = 1
   *
   */
  protected long[][][] content = new long[8][][];

  private long size = 0;
  protected Set<ORID> negatives = new HashSet<>();

  protected int maxArraySize;

  /** instantiates an ORidSet with a bucket size of Integer.MAX_VALUE / 10 */
  public ORidSet() {
    this(Integer.MAX_VALUE / 10);
  }

  /** @param bucketSize */
  public ORidSet(int bucketSize) {
    maxArraySize = bucketSize;
  }

  @Override
  public int size() {
    return size + negatives.size() <= Integer.MAX_VALUE
        ? (int) size + negatives.size()
        : Integer.MAX_VALUE;
  }

  @Override
  public boolean isEmpty() {
    return size == 0L;
  }

  @Override
  public boolean contains(Object o) {
    if (size == 0L && negatives.size() == 0) {
      return false;
    }
    if (!(o instanceof ORID)) {
      throw new IllegalArgumentException();
    }
    ORID identifiable = ((ORID) o);
    if (identifiable == null) {
      throw new IllegalArgumentException();
    }
    int cluster = identifiable.getClusterId();
    long position = identifiable.getClusterPosition();
    if (cluster < 0 || position < 0) {
      return negatives.contains(identifiable);
    }
    long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    int block = (int) (positionByte / maxArraySize);
    int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= cluster) {
      return false;
    }
    if (content[cluster] == null) {
      return false;
    }
    if (content[cluster].length <= block) {
      return false;
    }
    if (content[cluster][block] == null) {
      return false;
    }
    if (content[cluster][block].length <= blockPositionByteInt) {
      return false;
    }

    long currentMask = 1L << positionBit;
    long existed = content[cluster][block][blockPositionByteInt] & currentMask;

    return existed > 0L;
  }

  @Override
  public Iterator<ORID> iterator() {
    return new ORidSetIterator(this);
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean add(ORID identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException();
    }
    int cluster = identifiable.getClusterId();
    long position = identifiable.getClusterPosition();
    if (cluster < 0 || position < 0) {
      return negatives.add(identifiable);
    }
    long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    int block = (int) (positionByte / maxArraySize);
    int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= cluster) {
      long[][][] oldContent = content;
      content = new long[cluster + 1][][];
      System.arraycopy(oldContent, 0, content, 0, oldContent.length);
    }
    if (content[cluster] == null) {
      content[cluster] = createClusterArray(block, blockPositionByteInt);
    }

    if (content[cluster].length <= block) {
      content[cluster] = expandClusterBlocks(content[cluster], block, blockPositionByteInt);
    }
    if (content[cluster][block] == null) {
      content[cluster][block] =
          expandClusterArray(new long[INITIAL_BLOCK_SIZE], blockPositionByteInt);
    }
    if (content[cluster][block].length <= blockPositionByteInt) {
      content[cluster][block] = expandClusterArray(content[cluster][block], blockPositionByteInt);
    }

    long original = content[cluster][block][blockPositionByteInt];
    long currentMask = 1L << positionBit;
    long existed = content[cluster][block][blockPositionByteInt] & currentMask;
    content[cluster][block][blockPositionByteInt] = original | currentMask;
    if (existed == 0L) {
      size++;
    }
    return existed == 0L;
  }

  private static long[][] expandClusterBlocks(long[][] longs, int block, int blockPositionByteInt) {
    long[][] result = new long[block + 1][];
    System.arraycopy(longs, 0, result, 0, longs.length);
    result[block] = expandClusterArray(new long[INITIAL_BLOCK_SIZE], blockPositionByteInt);
    return result;
  }

  private static long[][] createClusterArray(int block, int positionByteInt) {
    int currentSize = INITIAL_BLOCK_SIZE;
    while (currentSize <= positionByteInt) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = positionByteInt + 1;
        break;
      }
    }
    long[][] result = new long[block + 1][];
    result[block] = new long[currentSize];
    return result;
  }

  private static long[] expandClusterArray(long[] original, int positionByteInt) {
    int currentSize = original.length;
    while (currentSize <= positionByteInt) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = positionByteInt + 1;
        break;
      }
    }
    long[] result = new long[currentSize];
    System.arraycopy(original, 0, result, 0, original.length);
    return result;
  }

  @Override
  public boolean remove(Object o) {
    if (!(o instanceof ORID)) {
      throw new IllegalArgumentException();
    }
    ORID identifiable = ((ORID) o);
    if (identifiable == null) {
      throw new IllegalArgumentException();
    }
    int cluster = identifiable.getClusterId();
    long position = identifiable.getClusterPosition();
    if (cluster < 0 || position < 0) {
      return negatives.remove(o);
    }
    long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    int block = (int) (positionByte / maxArraySize);
    int blockPositionByteInt = (int) (positionByte % maxArraySize);

    if (content.length <= cluster) {
      return false;
    }
    if (content[cluster] == null) {
      return false;
    }
    if (content[cluster].length <= block) {
      return false;
    }
    if (content[cluster][block].length <= blockPositionByteInt) {
      return false;
    }

    long original = content[cluster][block][blockPositionByteInt];
    long currentMask = 1L << positionBit;
    long existed = content[cluster][block][blockPositionByteInt] & currentMask;
    currentMask = ~currentMask;
    content[cluster][block][blockPositionByteInt] = original & currentMask;
    if (existed > 0) {
      size--;
    }
    return existed == 0L;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends ORID> c) {
    boolean added = false;
    for (ORID o : c) {
      added = added && add(o);
    }
    return added;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    for (Object o : c) {
      remove(o);
    }
    return true;
  }

  @Override
  public void clear() {
    content = new long[8][][];
    size = 0;
    this.negatives.clear();
  }
}
