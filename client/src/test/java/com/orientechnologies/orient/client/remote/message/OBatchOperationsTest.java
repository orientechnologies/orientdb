package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Created by Enrico Risa on 15/05/2017. */
public class OBatchOperationsTest {

  @Test
  public void testBatchOperationsNoTx() throws IOException {
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));

    MockChannel channel = new MockChannel();
    OBatchOperationsRequest request = new OBatchOperationsRequest(-1, operations);

    request.write(channel, null);

    channel.close();

    request = new OBatchOperationsRequest();

    request.read(channel, 0, ORecordSerializerNetworkFactory.INSTANCE.current());

    assertEquals(request.getOperations().size(), 1);
    assertEquals(request.getTxId(), -1);
  }
}
