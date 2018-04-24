package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Assert; import org.junit.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

public class OAtomicUnitEndRecordTest {
  @Test
  public void recordMetadataSerializationTest() {
    ORecordOperationMetadata recordOperationMetadata = new ORecordOperationMetadata();
    recordOperationMetadata.addRid(new ORecordId(10, 42));
    recordOperationMetadata.addRid(new ORecordId(42, 10));

    Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<String, OAtomicOperationMetadata<?>>();
    metadata.put(recordOperationMetadata.getKey(), recordOperationMetadata);

    OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(OOperationUnitId.generateId(), false, metadata);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    byte[] content = new byte[arraySize];

    final int endOffset = atomicUnitEndRecord.toStream(content, 1);
    Assert.assertEquals(endOffset, content.length);

    OAtomicUnitEndRecord atomicUnitEndRecordD = new OAtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content, 1);
    Assert.assertEquals(dEndOffset, content.length);

    Assert.assertEquals(atomicUnitEndRecordD.getOperationUnitId(), atomicUnitEndRecord.getOperationUnitId());
    ORecordOperationMetadata recordOperationMetadataD = (ORecordOperationMetadata) atomicUnitEndRecordD.getAtomicOperationMetadata()
        .get(ORecordOperationMetadata.RID_METADATA_KEY);

    Assert.assertEquals(recordOperationMetadataD.getValue(), recordOperationMetadata.getValue());
  }

  @Test
  public void recordMetadataSerializationTestBuffer() {
    ORecordOperationMetadata recordOperationMetadata = new ORecordOperationMetadata();
    recordOperationMetadata.addRid(new ORecordId(10, 42));
    recordOperationMetadata.addRid(new ORecordId(42, 10));

    Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<String, OAtomicOperationMetadata<?>>();
    metadata.put(recordOperationMetadata.getKey(), recordOperationMetadata);

    OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(OOperationUnitId.generateId(), false, metadata);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    ByteBuffer content = ByteBuffer.allocate(arraySize).order(ByteOrder.nativeOrder());

    content.position(1);
    atomicUnitEndRecord.toStream(content);
    Assert.assertEquals(arraySize, content.position());

    OAtomicUnitEndRecord atomicUnitEndRecordD = new OAtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content.array(), 1);
    Assert.assertEquals(dEndOffset, content.position());

    Assert.assertEquals(atomicUnitEndRecordD.getOperationUnitId(), atomicUnitEndRecord.getOperationUnitId());
    ORecordOperationMetadata recordOperationMetadataD = (ORecordOperationMetadata) atomicUnitEndRecordD.getAtomicOperationMetadata()
        .get(ORecordOperationMetadata.RID_METADATA_KEY);

    Assert.assertEquals(recordOperationMetadataD.getValue(), recordOperationMetadata.getValue());
  }

  @Test
  public void recordNoMetadataSerializationTest()  {
    OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(OOperationUnitId.generateId(), false, null);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    byte[] content = new byte[arraySize];

    final int endOffset = atomicUnitEndRecord.toStream(content, 1);
    Assert.assertEquals(endOffset, content.length);

    OAtomicUnitEndRecord atomicUnitEndRecordD = new OAtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content, 1);
    Assert.assertEquals(dEndOffset, content.length);
  }

  @Test
  public void recordNoMetadataSerializationTestBuffer()  {
    OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(OOperationUnitId.generateId(), false, null);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    ByteBuffer content = ByteBuffer.allocate(arraySize).order(ByteOrder.nativeOrder());

    content.position(1);
    atomicUnitEndRecord.toStream(content);
    Assert.assertEquals(arraySize, content.position());

    OAtomicUnitEndRecord atomicUnitEndRecordD = new OAtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content.array(), 1);
    Assert.assertEquals(dEndOffset, content.position());
  }
}
