package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.collection.OMultiValue;
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
public class OVersionDistributedConflictResolverTest {
  @Test
  public void winnerFound() throws Exception {
    final OVersionDistributedConflictResolver resolver = new OVersionDistributedConflictResolver();
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
    final OVersionDistributedConflictResolver resolver = new OVersionDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    // FILL CANDIDATES
    candidates.put(new ORawBuffer(new ODocument().fields("a", 4, "b", "yes").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 3, "b", "no").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), null, candidates);

    Assert.assertEquals(OContentDistributedConflictResolver.NOT_FOUND, result.winner);
  }
}