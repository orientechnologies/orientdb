package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by luigidellaquila on 25/10/16.
 */
public class ORidSet implements Set<ORID> {

  protected long[][] content = new long[8][];

  protected int  maxArraySize = Integer.MAX_VALUE / 10;
  protected long offset       = 0;

  ORidSet nextLevel;

  @Override public int size() {
    if (true) {
      return 0;//TODO
    }
    int result = 0;
    for (long[] block : content) {
      if (block != null) {
        for (long b : block) {
          for (int i = 0; i < 63; i++) {
            if ((1L & (b << i)) > 0) {
              result++;
            }
          }

        }
      }
    }
    return 0;
  }

  @Override public boolean isEmpty() {
    for (long[] block : content) {
      if (block != null) {
        for (long b : block) {
          if (b != 0L) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override public boolean contains(Object o) {
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
      return false;
    }
    long positionByte = position / 63;
    int positionBit = (int) (position % 63);
    if (positionByte > maxArraySize) {
      if (nextLevel == null) {
        return false;
      }

      return nextLevel.contains(new ORecordId(cluster, position - (((long) maxArraySize)) * 63));
    }
    int positionByteInt = (int) positionByte;

    if (content.length <= cluster) {
      return false;
    }
    if (content[cluster] == null) {
      return false;
    }
    if (content[cluster].length <= positionByteInt) {
      return false;
    }

    long currentMask = 1L << positionBit;
    long existed = content[cluster][positionByteInt] & currentMask;

    return existed > 0L;
  }

  @Override public Iterator<ORID> iterator() {
    return new ORidSetIterator(this);
  }

  @Override public Object[] toArray() {
    return new Object[0];
  }

  @Override public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override public boolean add(ORID identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException();
    }
    int cluster = identifiable.getClusterId();
    long position = identifiable.getClusterPosition();
    if (cluster < 0 || position < 0) {
      throw new IllegalArgumentException("negative RID");//TODO
    }
    long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    if (positionByte > maxArraySize) {
      if (nextLevel == null) {
        nextLevel = new ORidSet();
        nextLevel.offset = this.offset + maxArraySize;
      }
      return nextLevel.add(new ORecordId(cluster, position - (((long) maxArraySize)) * 63));
    }
    int positionByteInt = (int) positionByte;

    if (content.length <= cluster) {
      long[][] oldContent = content;
      content = new long[cluster + 1][];
      System.arraycopy(oldContent, 0, content, 0, oldContent.length);
    }
    if (content[cluster] == null) {
      content[cluster] = createClusterArray(positionByteInt);
    }
    if (content[cluster].length <= positionByteInt) {
      content[cluster] = expandClusterArray(content[cluster], positionByteInt);
    }

    long original = content[cluster][positionByteInt];
    long currentMask = 1L << positionBit;
    long existed = content[cluster][positionByteInt] & currentMask;
    content[cluster][positionByteInt] = original | currentMask;
    return existed == 0L;
  }

  private long[] createClusterArray(int positionByteInt) {
    int currentSize = 4096;
    while (currentSize <= positionByteInt) {
      currentSize *= 2;
      if (currentSize < 0) {
        currentSize = positionByteInt + 1;
        break;
      }
    }
    long[] result = new long[currentSize];
    return result;
  }

  private long[] expandClusterArray(long[] original, int positionByteInt) {
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

  @Override public boolean remove(Object o) {
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
      throw new IllegalArgumentException("negative RID");//TODO
    }
    long positionByte = (position / 63);
    int positionBit = (int) (position % 63);
    if (positionByte > maxArraySize) {
      if (nextLevel == null) {
        return false;
      }
      return nextLevel.remove(new ORecordId(cluster, position - (((long) maxArraySize)) * 63));
    }
    int positionByteInt = (int) positionByte;

    if (content.length <= cluster) {
      return false;
    }
    if (content[cluster] == null) {
      return false;
    }
    if (content[cluster].length <= positionByteInt) {
      return false;
    }

    long original = content[cluster][positionByteInt];
    long currentMask = 1L << positionBit;
    long existed = content[cluster][positionByteInt] & currentMask;
    currentMask = ~currentMask;
    content[cluster][positionByteInt] = original & currentMask;
    return existed == 0L;

  }

  @Override public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override public boolean addAll(Collection<? extends ORID> c) {
    boolean added = false;
    for (ORID o : c) {
      added = added && add(o);
    }
    return added;
  }

  @Override public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean removeAll(Collection<?> c) {
    for (Object o : c) {
      remove(o);
    }
    return true;
  }

  @Override public void clear() {
    throw new UnsupportedOperationException();
  }

}


