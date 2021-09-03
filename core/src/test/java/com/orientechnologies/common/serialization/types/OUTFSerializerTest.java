package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OUTFSerializerTest {
  byte[] stream;
  private String OBJECT;
  private OUTF8Serializer stringSerializer;

  @Before
  public void beforeClass() {
    OBJECT =
        "asd d astasf sdfrete 5678b sdf adfas ase sdf aas  t sdf ts d s e34523 sdf gsd 63 sdfs ыа ы кы афц"
            + "3м  ыпаыву  s sf s sdf asd asfsd w assf tet ы ц к к йцкуаыфв ыфафаф фаываыфав а фв аs  asf s sdfsa dscas "
            + " s as asdf sfsr43r344 1tasdf asa  asdfa fgwe treqr3 qadfasf аывфыфцк у фыва ые унпваыва  вайк ыавыфвауц";
    stringSerializer = new OUTF8Serializer();
  }

  @Test
  public void testSerialize() {
    stream = new byte[stringSerializer.getObjectSize(OBJECT) + 7];
    stringSerializer.serialize(OBJECT, stream, 7);
    Assert.assertEquals(stringSerializer.deserialize(stream, 7), OBJECT);
  }

  @Test
  public void testSerializeNative() {
    stream = new byte[stringSerializer.getObjectSize(OBJECT) + 7];
    stringSerializer.serializeNativeObject(OBJECT, stream, 7);
    Assert.assertEquals(stringSerializer.deserializeNativeObject(stream, 7), OBJECT);
  }

  @Test
  public void testSerializeNativeAsWhole() {
    stream = stringSerializer.serializeNativeAsWhole(OBJECT);
    Assert.assertEquals(stringSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  @Test
  public void testNativeDirectMemoryCompatibility() {
    stream = new byte[stringSerializer.getObjectSize(OBJECT) + 7];
    stringSerializer.serializeNativeObject(OBJECT, stream, 7);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(7);

    Assert.assertEquals(stringSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  @Test
  public void testNativeDirectMemoryCompatibilityAsWhole() {
    stream = stringSerializer.serializeNativeAsWhole(OBJECT);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(stringSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  @Test
  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocate(stringSerializer.getObjectSize(OBJECT) + serializationOffset);

    buffer.position(serializationOffset);
    stringSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, stringSerializer.getObjectSize(OBJECT));

    buffer.position(serializationOffset);
    Assert.assertEquals(
        stringSerializer.getObjectSizeInByteBuffer(buffer), stringSerializer.getObjectSize(OBJECT));

    buffer.position(serializationOffset);
    Assert.assertEquals(stringSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(
        buffer.position() - serializationOffset, stringSerializer.getObjectSize(OBJECT));
  }

  @Test
  public void testSerializeWALChanges() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(stringSerializer.getObjectSize(OBJECT) + serializationOffset)
            .order(ByteOrder.nativeOrder());

    final byte[] data = new byte[stringSerializer.getObjectSize(OBJECT)];
    stringSerializer.serializeNativeObject(OBJECT, data, 0);

    OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        stringSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        stringSerializer.getObjectSize(OBJECT));
    Assert.assertEquals(
        stringSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT);
  }
}
