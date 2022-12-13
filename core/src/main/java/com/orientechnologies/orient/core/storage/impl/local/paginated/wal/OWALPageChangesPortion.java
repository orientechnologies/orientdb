package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 8/17/2015
 */
public class OWALPageChangesPortion implements OWALChanges {
  private static final int PAGE_SIZE =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private static final int CHUNK_SIZE = 32;
  private static final int PORTION_SIZE = 32;
  static final int PORTION_BYTES = PORTION_SIZE * CHUNK_SIZE;

  private byte[][][] pageChunks;

  private final int pageSize;
  private final int chunksCount;

  public OWALPageChangesPortion() {
    this(PAGE_SIZE);
  }

  OWALPageChangesPortion(int pageSize) {
    this.pageSize = pageSize;
    this.chunksCount = (pageSize + (PORTION_BYTES - 1)) / PORTION_BYTES;
    if (pageSize % PORTION_BYTES != 0) {
      throw new IllegalArgumentException("Page size should be a multiple of " + PORTION_BYTES);
    }
  }

  @Override
  public void setLongValue(ByteBuffer pointer, long value, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  @Override
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

  @Override
  public void setByteValue(ByteBuffer pointer, byte value, int offset) {
    byte[] data = new byte[] {value};

    updateData(pointer, offset, data);
  }

  @Override
  public void setBinaryValue(ByteBuffer pointer, byte[] value, int offset) {
    updateData(pointer, offset, value);
  }

  @Override
  public void moveData(ByteBuffer pointer, int from, int to, int len) {
    byte[] buff = new byte[len];
    readData(pointer, from, buff);
    updateData(pointer, to, buff);
  }

  @Override
  public long getLongValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];

    readData(pointer, offset, data);

    return OLongSerializer.INSTANCE.deserializeNative(data, 0);
  }

  @Override
  public int getIntValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];

    readData(pointer, offset, data);

    return OIntegerSerializer.INSTANCE.deserializeNative(data, 0);
  }

  @Override
  public short getShortValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];

    readData(pointer, offset, data);

    return OShortSerializer.INSTANCE.deserializeNative(data, 0);
  }

  @Override
  public byte getByteValue(ByteBuffer pointer, int offset) {
    byte[] data = new byte[1];

    readData(pointer, offset, data);

    return data[0];
  }

  @Override
  public byte[] getBinaryValue(ByteBuffer pointer, int offset, int len) {
    byte[] data = new byte[len];
    readData(pointer, offset, data);

    return data;
  }

  @Override
  public void applyChanges(ByteBuffer pointer) {
    if (pageChunks == null) return;
    for (int i = 0; i < pageChunks.length; i++) {
      if (pageChunks[i] != null) {
        for (int j = 0; j < PORTION_SIZE; j++) {
          byte[] chunk = pageChunks[i][j];
          if (chunk != null) {
            pointer.position(i * PORTION_BYTES + j * CHUNK_SIZE);
            pointer.put(chunk, 0, chunk.length);
          }
        }
      }
    }
  }

  @Override
  public int serializedSize() {
    int offset;

    if (pageChunks == null) {
      offset = OShortSerializer.SHORT_SIZE;
    } else {
      offset = OShortSerializer.SHORT_SIZE;

      for (byte[][] pageChunk : pageChunks) {
        if (pageChunk != null) {
          for (int j = 0; j < PORTION_SIZE; j++) {
            if (pageChunk[j] != null) {
              offset += 2 * OByteSerializer.BYTE_SIZE + CHUNK_SIZE;
            }
          }
        }
      }
    }

    return offset;
  }

  @Override
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
            OByteSerializer.INSTANCE.serializeNative((byte) i, stream, offset);
            offset += OByteSerializer.BYTE_SIZE;
            OByteSerializer.INSTANCE.serializeNative((byte) j, stream, offset);
            offset += OByteSerializer.BYTE_SIZE;

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

  @Override
  public void toStream(ByteBuffer buffer) {
    if (pageChunks == null) {
      buffer.putShort((short) 0);
      return;
    }

    int countPos = buffer.position();
    buffer.position(countPos + OShortSerializer.SHORT_SIZE);
    int count = 0;

    for (int i = 0; i < pageChunks.length; i++) {
      if (pageChunks[i] != null) {
        for (int j = 0; j < PORTION_SIZE; j++) {
          if (pageChunks[i][j] != null) {
            buffer.put((byte) i);
            buffer.put((byte) j);

            buffer.put(pageChunks[i][j]);

            count++;
          }
        }
      }
    }

    buffer.putShort(countPos, (short) count);
  }

  @Override
  public int fromStream(int offset, byte[] stream) {
    int chunkLength = OShortSerializer.INSTANCE.deserializeNative(stream, offset);

    offset += OShortSerializer.SHORT_SIZE;

    for (int c = 0; c < chunkLength; c++) {
      int i = OByteSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OByteSerializer.BYTE_SIZE;
      int j = OByteSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OByteSerializer.BYTE_SIZE;

      if (pageChunks == null) {
        pageChunks = new byte[(pageSize + (PORTION_BYTES - 1)) / PORTION_BYTES][][];
      }
      if (pageChunks[i] == null) {
        pageChunks[i] = new byte[PORTION_SIZE][];
      }

      if (pageChunks[i][j] == null) {
        pageChunks[i][j] = new byte[CHUNK_SIZE];
      }

      System.arraycopy(stream, offset, pageChunks[i][j], 0, CHUNK_SIZE);
      offset += CHUNK_SIZE;
    }
    return offset;
  }

  @Override
  public void fromStream(ByteBuffer buffer) {
    int chunkLength = buffer.getShort();

    for (int c = 0; c < chunkLength; c++) {
      int i = buffer.get();
      int j = buffer.get();

      if (pageChunks == null) {
        pageChunks = new byte[(pageSize + (PORTION_BYTES - 1)) / PORTION_BYTES][][];
      }
      if (pageChunks[i] == null) {
        pageChunks[i] = new byte[PORTION_SIZE][];
      }

      if (pageChunks[i][j] == null) {
        pageChunks[i][j] = new byte[CHUNK_SIZE];
      }

      buffer.get(pageChunks[i][j]);
    }
  }

  private void readData(ByteBuffer pointer, int offset, byte[] data) {
    if (pageChunks == null) {
      if (pointer != null) {
        pointer.position(offset);
        pointer.get(data, 0, data.length);
      }
      return;
    }

    int portionIndex = offset / PORTION_BYTES;
    if (portionIndex == (offset + data.length - 1) / PORTION_BYTES
        && pageChunks[portionIndex] == null) {
      if (pointer != null) {
        pointer.position(offset);
        pointer.get(data, 0, data.length);
      }
      return;
    }

    int chunkIndex = (offset - portionIndex * PORTION_BYTES) / CHUNK_SIZE;
    int chunkOffset = offset - (portionIndex * PORTION_BYTES + chunkIndex * CHUNK_SIZE);

    int read = 0;

    while (read < data.length) {
      byte[] chunk = null;
      if (pageChunks[portionIndex] != null) chunk = pageChunks[portionIndex][chunkIndex];

      final int rl = Math.min(CHUNK_SIZE - chunkOffset, data.length - read);
      if (chunk == null) {
        if (pointer != null) {
          pointer.position(portionIndex * PORTION_BYTES + (chunkIndex * CHUNK_SIZE) + chunkOffset);
          pointer.get(data, read, rl);
        }
      } else
        try {
          System.arraycopy(chunk, chunkOffset, data, read, rl);
        } catch (Exception e) {
          OLogManager.instance()
              .error(
                  this,
                  "System.arraycopy error: chunk.length = "
                      + chunk.length
                      + ", chunkOffset = "
                      + chunkOffset
                      + ", data.length = "
                      + data.length
                      + ", read = "
                      + read
                      + ", rl = "
                      + rl,
                  e);
          throw e;
        }

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
      pageChunks = new byte[this.chunksCount][][];
    }

    int portionIndex = offset / PORTION_BYTES;
    assert portionIndex < pageChunks.length;
    assert (pageChunks.length - portionIndex) * PORTION_BYTES >= data.length
        : "wrong portionIndex:" + portionIndex + " data:" + data.length;

    if (pageChunks[portionIndex] == null) {
      pageChunks[portionIndex] = new byte[PORTION_SIZE][];
    }

    int chunkIndex = (offset - portionIndex * PORTION_BYTES) / CHUNK_SIZE;
    int chunkOffset = offset - (portionIndex * PORTION_BYTES + chunkIndex * CHUNK_SIZE);

    int written = 0;

    while (written < data.length) {
      byte[] chunk = pageChunks[portionIndex][chunkIndex];

      if (chunk == null) {
        chunk = new byte[CHUNK_SIZE];

        // pointer can be null for new pages
        if (pointer != null) {
          pointer.position(portionIndex * PORTION_BYTES + chunkIndex * CHUNK_SIZE);
          pointer.get(chunk);
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

        if (pageChunks[portionIndex] == null) {
          pageChunks[portionIndex] = new byte[PORTION_SIZE][];
        }

        chunkIndex = 0;
      }
    }
  }

  @Override
  public boolean hasChanges() {
    return pageChunks != null;
  }
}
