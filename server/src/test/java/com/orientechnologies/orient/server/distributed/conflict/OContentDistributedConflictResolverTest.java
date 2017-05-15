package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luca on 13/05/17.
 */
public class OContentDistributedConflictResolverTest {
  @Test
  public void winnerFound() throws Exception {
    final OContentDistributedConflictResolver resolver = new OContentDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    final ODocument expectedWinnerRecord = new ODocument().fields("a", 3, "b", "yes");

    // FILL CANDIDATES
    candidates
        .put(new ORawBuffer(expectedWinnerRecord.toStream(), 1, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("server0"));
    candidates
        .put(new ORawBuffer(expectedWinnerRecord.toStream(), 2, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("server1"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 4, "b", "yes").toStream(), 3, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 3, "b", "no").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates
        .put(new ORawBuffer(expectedWinnerRecord.toStream(), 5, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("server3"));

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), null, candidates);

    Assert.assertNotNull(result.winner);
    Assert.assertTrue(result.winner instanceof ORawBuffer);

    final ODocument winnerRecord = new ODocument().fromStream(((ORawBuffer) result.winner).buffer);

    Assert.assertTrue(winnerRecord.hasSameContentOf(expectedWinnerRecord));
    Assert.assertEquals(5, ((ORawBuffer) result.winner).version);
  }

  @Test
  public void winnerNotFound() throws Exception {
    final OContentDistributedConflictResolver resolver = new OContentDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    // FILL CANDIDATES
    candidates.put(new ORawBuffer(new ODocument().fields("a", 4, "b", "yes").toStream(), 3, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 3, "b", "no").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), null, candidates);

    Assert.assertEquals(OContentDistributedConflictResolver.NOT_FOUND, result.winner);
  }

  @Test
  public void winnerFoundBinaryValuesNotTheSame() throws Exception {
    final OContentDistributedConflictResolver resolver = new OContentDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    // FILL CANDIDATES WITH REAL BINARY VALUES TAKEN FROM A RUNNING TEST
    final ODocument doc1 = new ODocument().fromStream(
        new byte[] { 0, 2, 86, 8, 111, 117, 116, 95, 0, 0, 0, 14, 22, 0, 1, 0, 0, 0, 13, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 24, 0, 0, 0, 0, 0, 0, 0, 2, 0, 24, 0, 0, 0, 0, 0, 0, 0, 3, 0, 17, 0, 0, 0, 0, 0, 0, 0, 1, 0, 21,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 1, 0, 21, 0, 0, 0, 0, 0, 0, 0, 2, 0, 24, 0, 0, 0, 0, 0, 0, 0, 6 });
    final ODocument doc2 = new ODocument().fromStream(
        new byte[] { 0, 2, 86, 8, 111, 117, 116, 95, 0, 0, 0, 14, 22, 0, 1, 0, 0, 0, 12, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 24, 0, 0, 0, 0, 0, 0, 0, 2, 0, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 2, 0, 23,
            0, 0, 0, 0, 0, 0, 0, 3, 0, 19, 0, 0, 0, 0, 0, 0, 0, 5, 0, 19, 0, 0, 0, 0, 0, 0, 0, 7 });
    final ODocument doc3 = new ODocument().fromStream(
        new byte[] { 0, 2, 86, 8, 111, 117, 116, 95, 0, 0, 0, 14, 22, 0, 1, 0, 0, 0, 13, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 24, 0, 0, 0, 0, 0, 0, 0, 2, 0, 24, 0, 0, 0, 0, 0, 0, 0, 3, 0, 17, 0, 0, 0, 0, 0, 0, 0, 1, 0, 21,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 1, 0, 21, 0, 0, 0, 0, 0, 0, 0, 2, 0, 20, 0, 0, 0, 0, 0, 0, 0, 4 });

    candidates.put(new ORawBuffer(doc1.toStream(), 3, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("node1"));
    candidates.put(new ORawBuffer(doc2.toStream(), 3, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("node2"));
    candidates.put(new ORawBuffer(doc3.toStream(), 3, ODocument.RECORD_TYPE), OMultiValue.getSingletonList("node3"));

    OLogManager.instance().info(this, "doc1=" + doc1);
    OLogManager.instance().info(this, "doc2=" + doc2);
    OLogManager.instance().info(this, "doc3=" + doc3);

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), null, candidates);

    Assert.assertEquals(ODistributedConflictResolver.NOT_FOUND, result.winner);
  }
}