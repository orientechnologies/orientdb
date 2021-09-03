package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Test;

public class EdgeKeySerializerTest {

  @Test
  public void testSerialization() {
    final EdgeKey edgeKey = new EdgeKey(42, 24, 67);
    final EdgeKeySerializer edgeKeySerializer = new EdgeKeySerializer();

    final int serializedSize = edgeKeySerializer.getObjectSize(edgeKey);
    final byte[] rawKey = new byte[serializedSize + 3];

    edgeKeySerializer.serialize(edgeKey, rawKey, 3);

    Assert.assertEquals(serializedSize, edgeKeySerializer.getObjectSize(rawKey, 3));

    final EdgeKey deserializedKey = edgeKeySerializer.deserialize(rawKey, 3);

    Assert.assertEquals(edgeKey, deserializedKey);
  }

  @Test
  public void testBufferSerialization() {
    final EdgeKey edgeKey = new EdgeKey(42, 24, 67);
    final EdgeKeySerializer edgeKeySerializer = new EdgeKeySerializer();

    final int serializedSize = edgeKeySerializer.getObjectSize(edgeKey);
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 3);

    buffer.position(3);
    edgeKeySerializer.serializeInByteBufferObject(edgeKey, buffer);

    Assert.assertEquals(3 + serializedSize, buffer.position());

    buffer.position(3);
    Assert.assertEquals(serializedSize, edgeKeySerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(3);
    final EdgeKey deserializedKey = edgeKeySerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(edgeKey, deserializedKey);
  }

  @Test
  public void testChangesSerialization() {
    final EdgeKey edgeKey = new EdgeKey(42, 24, 67);
    final EdgeKeySerializer edgeKeySerializer = new EdgeKeySerializer();

    final int serializedSize = edgeKeySerializer.getObjectSize(edgeKey);

    final OWALChanges walChanges = new OWALPageChangesPortion();
    final ByteBuffer buffer =
        ByteBuffer.allocate(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024)
            .order(ByteOrder.nativeOrder());

    final byte[] rawKey = new byte[serializedSize];

    edgeKeySerializer.serialize(edgeKey, rawKey, 0);
    walChanges.setBinaryValue(buffer, rawKey, 3);

    Assert.assertEquals(
        serializedSize, edgeKeySerializer.getObjectSizeInByteBuffer(buffer, walChanges, 3));

    final EdgeKey deserializedKey =
        edgeKeySerializer.deserializeFromByteBufferObject(buffer, walChanges, 3);

    Assert.assertEquals(edgeKey, deserializedKey);
  }
}
