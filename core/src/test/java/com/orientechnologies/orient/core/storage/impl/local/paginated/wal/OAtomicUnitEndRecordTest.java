package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test
public class OAtomicUnitEndRecordTest {
  public void recordMetadataSerializationTest() {
    ORecordOperationMetadata recordOperationMetadata = new ORecordOperationMetadata();
    recordOperationMetadata.addRid(new ORecordId(10, 42));
    recordOperationMetadata.addRid(new ORecordId(42, 10));

    Map<String, OAtomicOperationMetadata<?>> metadata = new HashMap<String, OAtomicOperationMetadata<?>>();
    metadata.put(recordOperationMetadata.getKey(), recordOperationMetadata);

    OAtomicUnitEndRecord atomicUnitEndRecord = new OAtomicUnitEndRecord(OOperationUnitId.generateId(), false, metadata);
    int arraySize = atomicUnitEndRecord.serializedSize() + 1;
    byte[] content = new byte[arraySize];

    final int endOffset = atomicUnitEndRecord.toStream(content, 1);
    Assert.assertEquals(endOffset, content.length);

    OAtomicUnitEndRecord atomicUnitEndRecordD = new OAtomicUnitEndRecord();
    final int dEndOffset = atomicUnitEndRecordD.fromStream(content, 1);
    Assert.assertEquals(dEndOffset, content.length);
  }
}
