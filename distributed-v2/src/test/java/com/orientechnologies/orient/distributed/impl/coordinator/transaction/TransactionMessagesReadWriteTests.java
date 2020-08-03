package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OExceptionResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TransactionMessagesReadWriteTests {

  @Test
  public void testFirstPhaseSuccess() {

    OTransactionFirstPhaseResult result =
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.SUCCESS, null);
    OTransactionFirstPhaseResult readResult = new OTransactionFirstPhaseResult();
    writeRead(result, readResult);
    assertEquals(result.getType(), readResult.getType());
  }

  @Test
  public void testFirstPhaseConcurrentModification() {
    OConcurrentModificationResult payload =
        new OConcurrentModificationResult(new ORecordId(10, 10), 10, 20);
    OTransactionFirstPhaseResult result =
        new OTransactionFirstPhaseResult(
            OTransactionFirstPhaseResult.Type.CONCURRENT_MODIFICATION_EXCEPTION, payload);
    OTransactionFirstPhaseResult readResult = new OTransactionFirstPhaseResult();
    writeRead(result, readResult);
    assertEquals(result.getType(), readResult.getType());

    OConcurrentModificationResult readMetadata =
        (OConcurrentModificationResult) readResult.getResultMetadata();
    assertEquals(payload.getRecordId(), readMetadata.getRecordId());
    assertEquals(payload.getPersistentVersion(), readMetadata.getPersistentVersion());
    assertEquals(payload.getUpdateVersion(), readMetadata.getUpdateVersion());
  }

  @Test
  public void testFirstPhaseUniqueIndex() {
    OUniqueKeyViolationResult payload =
        new OUniqueKeyViolationResult(
            "hello", new ORecordId(10, 10), new ORecordId(10, 11), "test.index");
    OTransactionFirstPhaseResult result =
        new OTransactionFirstPhaseResult(
            OTransactionFirstPhaseResult.Type.UNIQUE_KEY_VIOLATION, payload);
    OTransactionFirstPhaseResult readResult = new OTransactionFirstPhaseResult();
    writeRead(result, readResult);
    assertEquals(result.getType(), readResult.getType());

    OUniqueKeyViolationResult readMetadata =
        (OUniqueKeyViolationResult) readResult.getResultMetadata();
    assertEquals(payload.getKeyStringified(), readMetadata.getKeyStringified());
    assertEquals(payload.getIndexName(), readMetadata.getIndexName());
    assertEquals(payload.getRecordOwner(), readMetadata.getRecordOwner());
    assertEquals(payload.getRecordRequesting(), readMetadata.getRecordRequesting());
  }

  @Test
  public void testFirstPhaseException() {
    OExceptionResult payload = new OExceptionResult(new RuntimeException("test"));
    OTransactionFirstPhaseResult result =
        new OTransactionFirstPhaseResult(OTransactionFirstPhaseResult.Type.EXCEPTION, payload);
    OTransactionFirstPhaseResult readResult = new OTransactionFirstPhaseResult();
    writeRead(result, readResult);
    assertEquals(result.getType(), readResult.getType());

    OExceptionResult readMetadata = (OExceptionResult) readResult.getResultMetadata();
    assertEquals(payload.getException().getMessage(), readMetadata.getException().getMessage());
  }

  @Test
  public void testSecondPhase() {
    final OTransactionSecondPhaseOperation operation =
        new OTransactionSecondPhaseOperation(
            new OSessionOperationId(), new ArrayList<>(), new ArrayList<>(), true);
    final OTransactionSecondPhaseOperation readOperation = new OTransactionSecondPhaseOperation();
    writeRead(operation, readOperation);
    // assertEquals(operation.getOperationId(), readOperation.getOperationId());
    assertEquals(operation.isSuccess(), readOperation.isSuccess());
  }

  @Test
  public void testSecondPhaseResult() {
    final OTransactionSecondPhaseResponse operation =
        new OTransactionSecondPhaseResponse(
            true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    final OTransactionSecondPhaseResponse readOperation = new OTransactionSecondPhaseResponse();
    writeRead(operation, readOperation);
    assertEquals(operation.isSuccess(), readOperation.isSuccess());
  }

  @Test
  public void testFirstPhase() {
    final List<ORecordOperationRequest> records = new ArrayList<>();
    final ORecordOperationRequest recordOperation =
        new ORecordOperationRequest(
            ORecordOperation.CREATED,
            (byte) 'a',
            new ORecordId(10, 10),
            new ORecordId(10, 11),
            "bytes".getBytes(),
            10,
            true);
    records.add(recordOperation);

    final OIndexKeyOperation indexOp =
        new OIndexKeyOperation(OIndexKeyOperation.PUT, new ORecordId(20, 30));
    final List<OIndexKeyOperation> keyOps = new ArrayList<>();
    keyOps.add(indexOp);

    final OIndexKeyChange keyChange = new OIndexKeyChange("string", keyOps);
    final List<OIndexKeyChange> indexChanges = new ArrayList<>();
    indexChanges.add(keyChange);
    final OIndexOperationRequest indexOperation =
        new OIndexOperationRequest("one", true, indexChanges);

    final List<OIndexOperationRequest> indexes = new ArrayList<>();
    indexes.add(indexOperation);

    final OTransactionFirstPhaseOperation operation =
        new OTransactionFirstPhaseOperation(new OSessionOperationId(), records, indexes);
    final OTransactionFirstPhaseOperation readOperation = new OTransactionFirstPhaseOperation();

    writeRead(operation, readOperation);

    assertEquals(readOperation.getOperations().size(), 1);
    final ORecordOperationRequest readRec = readOperation.getOperations().get(0);
    assertEquals(readRec.getType(), ORecordOperation.CREATED);
    assertEquals(readRec.getRecordType(), 'a');
    assertEquals(readRec.getId(), new ORecordId(10, 10));
    assertEquals(readRec.getOldId(), new ORecordId(10, 11));
    assertEquals(readRec.getVersion(), 10);

    assertEquals(readOperation.getIndexes().size(), 1);
    final OIndexOperationRequest readIndex = readOperation.getIndexes().get(0);
    assertEquals(readIndex.getIndexName(), "one");
    assertTrue(readIndex.isCleanIndexValues());
    assertEquals(readIndex.getIndexKeyChanges().size(), 1);
    final OIndexKeyChange readChange = readIndex.getIndexKeyChanges().get(0);
    assertEquals(readChange.getKey(), "string");
    assertEquals(readChange.getOperations().size(), 1);
    final OIndexKeyOperation readKeyOp = readChange.getOperations().get(0);
    assertEquals(readKeyOp.getType(), OIndexKeyOperation.PUT);
    assertEquals(readKeyOp.getValue(), new ORecordId(20, 30));
  }

  private static void writeRead(ONodeRequest operation, ONodeRequest readOperation) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      operation.serialize(new DataOutputStream(outputStream));
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      readOperation.deserialize(new DataInputStream(inputStream));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private static void writeRead(ONodeResponse result, ONodeResponse readResult) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      result.serialize(new DataOutputStream(outputStream));
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      readResult.deserialize(new DataInputStream(inputStream));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
