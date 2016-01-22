package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/17/2015
 */
public class OWALPageChangesPortion implements  OWALChanges {
  private static final int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int CHUNK_SIZE = 32;
  public static final int PORTION_SIZE = 32;
  public static final int PORTION_BYTES = PORTION_SIZE * CHUNK_SIZE;

  private byte[][][] pageChunks;
  private final int      pageSize;

  public OWALPageChangesPortion() {
    this(PAGE_SIZE);
  }

  public OWALPageChangesPortion(int pageSize) {
    this.pageSize = pageSize;
  }

  public void setLongValue(ByteBuffer pointer, long value, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setIntValue(ByteBuffer pointer, int value, int offset) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setShortValue(ByteBuffer pointer, short value, int offset) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];
    OShortSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setByteValue(ByteBuffer pointer, byte value, int offset) {
    byte[] data = new byte[] { value };

    updateData(pointer, offset, data);
  }

  public void setBinaryValue(ByteBuffer pointer, byte[] value, int offset) {
    updateData(pointer, offset, value);
  }

  public void moveData(ByteBuffer pointer, int from, int to, int len) {
    byte [] buff = new byte[len];
    readData(pointer, from, buff);
    updateData(pointer, to, buff);
  }

  public long getLongValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];

    readData(pointer, offset, data);

    return OLongSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public int getIntValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];

    readData(pointer, offset, data);

    return OIntegerSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public short getShortValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];

    readData(pointer, offset, data);

    return OShortSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public byte getByteValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[1];

    readData(pointer, offset, data);

    return data[0];
  }

  public byte[] getBinaryValue(ByteBuffer pointer, int offset, int len) {
    byte[] data = new byte[len];
    readData(pointer, offset, data);

    return data;
  }

  public void applyChanges(ByteBuffer pointer) {
    if (pageChunks == null)
      return;
    for (int i = 0; i < pageChunks.length; i++) {
      if(pageChunks[i]!=null) {
        for (int j = 0; j < PORTION_SIZE; j++) {
          byte[] chunk = pageChunks[i][j];
          if (chunk != null) {
            if (i < pageChunks.length - 1 || j < PORTION_SIZE - 1) {
              pointer.position(i * PORTION_BYTES + j * CHUNK_SIZE);
              pointer.put(chunk, 0, chunk.length);
            } else {
              final int wl = Math.min(chunk.length, pageSize - ((pageChunks.length - 1) * PORTION_BYTES + (PORTION_SIZE - 1) * CHUNK_SIZE));
              pointer.position(i * PORTION_BYTES + j * CHUNK_SIZE);
              pointer.put(chunk, 0, wl);
            }
          }
        }
      }
    }
  }

  public int serializedSize() {
    if (pageChunks == null) {
      return OShortSerializer.SHORT_SIZE;
    }
    int offset = 0;
    offset += OShortSerializer.SHORT_SIZE;
    for (int i = 0; i < pageChunks.length; i++) {
      if (pageChunks[i] != null) {
        for (int j = 0; j < PORTION_SIZE; j++) {
          if (pageChunks[i][j] != null) {
            offset += OShortSerializer.SHORT_SIZE;
            offset += CHUNK_SIZE;
          }
        }
      }
    }
    return offset;
  }

  public int toStream(int offset, byte[] stream) {
    if (pageChunks == null) {
      OShortSerializer.INSTANCE.serializeNative((short) 0, stream, offset);
      return offset + OShortSerializer.SHORT_SIZE;
    }
    int countPos = offset;
    int count = 0;
    offset += OShortSerializer.SHORT_SIZE;
    for (int i = 0; i < pageChunks.length; i++) {
      if (pageChunks[i] != null) {
        for (int j = 0; j < PORTION_SIZE; j++) {
          if (pageChunks[i][j] != null) {
            OShortSerializer.INSTANCE.serializeNative((short) (i * PORTION_BYTES + j * CHUNK_SIZE), stream, offset);
            offset += OShortSerializer.SHORT_SIZE;
            System.arraycopy(pageChunks[i][j], 0, stream, offset, CHUNK_SIZE);
            offset += CHUNK_SIZE;
            count++;
          }
        }
      }
    }
    OShortSerializer.INSTANCE.serializeNative((short) count, stream, countPos);
    return offset;
  }

  public int fromStream(int offset, byte[] stream) {

    /*if (stream[offset] == 0)
      return offset + 1;
    boolean isNull = stream[offset] == 0;
    offset++;

    int chunkLength;
    int chunkIndex = 0;

    do {

      chunkLength = OShortSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OShortSerializer.SHORT_SIZE;

      if (!isNull) {
        for (int i = 0; i < chunkLength; i++) {
          for (int j = 0 ; j<PORTION_SIZE; j++) {
            byte[] chunk = new byte[CHUNK_SIZE];
            System.arraycopy(stream, offset, chunk, 0, CHUNK_SIZE);

            pageChunks[chunkIndex] = chunk;
            chunkIndex++;

            offset += CHUNK_SIZE;
          }
        }
      } else {
        chunkIndex += chunkLength;
      }

      isNull = !isNull;
    } while (chunkIndex < pageChunks.length);
*/
    return offset;
  }

  private void readData(ByteBuffer pointer, int offset, byte[] data) {
    if (pageChunks == null  || pageChunks[offset / PORTION_BYTES] == null) {
      if(pointer != null) {
        pointer.position(offset);
        pointer.get(data, 0, data.length);
      }
      return;
    }
    int portionIndex = offset / PORTION_BYTES;
    int chunkIndex = (offset - portionIndex * PORTION_BYTES) / CHUNK_SIZE;
    int chunkOffset = offset - (portionIndex * PORTION_BYTES + chunkIndex * CHUNK_SIZE);

    int read = 0;

    while (read < data.length) {
      byte[] chunk = null;
      if (pageChunks[portionIndex] != null)
        chunk = pageChunks[portionIndex][chunkIndex];

      final int rl = Math.min(CHUNK_SIZE - chunkOffset, data.length - read);
      if (chunk == null) {
        if (pointer != null) {
          if (portionIndex < pageChunks.length - 1 || chunkIndex < PORTION_SIZE - 1) {
            pointer.position(portionIndex * PORTION_BYTES + (chunkIndex * CHUNK_SIZE)+ chunkOffset);
            pointer.get( data, read, rl);
          } else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - ((pageChunks.length - 1) * PORTION_BYTES + (PORTION_SIZE - 1) * CHUNK_SIZE));

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;

            final int toRead = Math.min(rl, chunkSize);
            pointer.position(portionIndex * PORTION_BYTES + (chunkIndex * CHUNK_SIZE) + chunkOffset);
            pointer.get( data, read, toRead);
          }
        }
      } else
        System.arraycopy(chunk, chunkOffset, data, read, rl);

      read += rl;
      chunkOffset = 0;
      chunkIndex++;
      if (chunkIndex == PORTION_SIZE && read < data.length) {
        portionIndex++;
        chunkIndex = 0;
      }
    }
  }

  private void updateData(ByteBuffer pointer, int offset, byte[] data) {
    if (pageChunks == null) {
      pageChunks = new byte[(pageSize + (PORTION_BYTES - 1)) / PORTION_BYTES][][];
    }
    int portionIndex = offset / PORTION_BYTES;
    if(pageChunks[portionIndex] == null){
      pageChunks[portionIndex] = new byte[PORTION_SIZE][];
    }
    int chunkIndex = (offset - portionIndex * PORTION_BYTES) / CHUNK_SIZE;
    int chunkOffset = offset - (portionIndex * PORTION_BYTES + chunkIndex * CHUNK_SIZE);

    int written = 0;

    while (written < data.length) {
      byte[] chunk = pageChunks[portionIndex][chunkIndex];

      if (chunk == null) {
        if (pointer != null) {
          if (portionIndex < pageChunks.length - 1|| chunkIndex < PORTION_SIZE - 1) {
            pointer.position(portionIndex * PORTION_BYTES + (chunkIndex) * CHUNK_SIZE);
            chunk = new byte[CHUNK_SIZE];
            pointer.get(chunk);
          } else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - ((pageChunks.length - 1) * PORTION_BYTES + (PORTION_SIZE - 1) * CHUNK_SIZE));
            chunk = new byte[CHUNK_SIZE];

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;
            pointer.position(portionIndex * PORTION_BYTES + (chunkIndex * CHUNK_SIZE));
            pointer.get( chunk, 0, chunkSize);
          }
        } else {
          chunk = new byte[CHUNK_SIZE];
        }

        pageChunks[portionIndex][chunkIndex] = chunk;
      }

      final int wl = Math.min(CHUNK_SIZE - chunkOffset, data.length - written);
      System.arraycopy(data, written, chunk, chunkOffset, wl);

      written += wl;
      chunkOffset = 0;
      chunkIndex++;
      if (chunkIndex == PORTION_SIZE && written < data.length) {
        portionIndex++;
        if (pageChunks[portionIndex] == null)
          pageChunks[portionIndex] = new byte[PORTION_SIZE][];
        chunkIndex = 0;
      }
    }
  }
}
