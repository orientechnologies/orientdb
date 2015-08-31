package com.orientechnologies.orient.server.distributed.sequence;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public abstract class AbstractServerClusterSequenceTest extends AbstractServerClusterTest {
  private static final boolean                  RUN_PARALLEL_SYNC_TEST = true;
  private static final int                      SEQ_RUN_COUNT          = 100;

  private static final int                      DB_COUNT               = 2;
  private static final int                      THREAD_POOL_SIZE       = 2;
  private static final int                      CACHE_SIZE             = 27;

  private AtomicLong                            failures               = new AtomicLong();

  private final OPartitionedDatabasePoolFactory poolFactory            = new OPartitionedDatabasePoolFactory();

  @Override
  public String getDatabaseName() {
    return "distributed";
  }

  protected abstract String getDatabaseURL(ServerRun server);

  @Override
  public void executeTest() throws Exception {
    // Test two instances only;
    Assert.assertTrue("Test must run with at least 2 dbs", DB_COUNT >= 2);
    final ODatabaseDocumentTx[] dbs = new ODatabaseDocumentTx[DB_COUNT];
    for (int i = 0; i < DB_COUNT; ++i) {
      dbs[i] = poolFactory.get(getDatabaseURL(serverInstance.get(i)), "admin", "admin").acquire();
    }

    executeOrderedSequenceTest(dbs, "seq0");

    executeCachedSequenceTest(dbs, "seq1");
  }

  private void executeCachedSequenceTest(final ODatabaseDocumentTx[] dbs, final String sequenceName) {
    // Assuming seq2.next() is called once after calling seq1.next() once, and cache size is
    // C, seq2.current() - seq1.current() = C

    OSequence seq1 = dbs[0].getMetadata().getSequenceLibrary()
        .createSequence(sequenceName, SEQUENCE_TYPE.CACHED, new OSequence.CreateParams().setDefaults().setCacheSize(CACHE_SIZE));
    OSequence seq2 = dbs[1].getMetadata().getSequenceLibrary().getSequence(sequenceName);

    Assert.assertEquals(seq1.getName(), seq2.getName());
    Assert.assertEquals(seq1.getName(), sequenceName);

    Assert.assertEquals(seq1.getSequenceType(), seq2.getSequenceType());
    Assert.assertEquals(seq1.getSequenceType(), SEQUENCE_TYPE.CACHED);

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[0]);
    long v1 = seq1.next();

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[1]);
    long v2 = seq2.next();

    Assert.assertEquals((long) CACHE_SIZE, v2 - v1);
  }

  private void executeOrderedSequenceTest(final ODatabaseDocumentTx[] dbs, final String sequenceName) throws Exception {
    OSequenceLibrary seq1 = dbs[0].getMetadata().getSequenceLibrary();
    OSequenceLibrary seq2 = dbs[1].getMetadata().getSequenceLibrary();

    //
    seq1.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, null);
    Assert.assertEquals(SEQUENCE_TYPE.ORDERED, seq1.getSequence(sequenceName).getSequenceType());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[0]);
    Assert.assertEquals(0L, seq1.getSequence(sequenceName).current());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[1]);
    Assert.assertEquals(0L, seq2.getSequence(sequenceName).current());
    Assert.assertEquals(1L, seq2.getSequence(sequenceName).next());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[0]);
    Assert.assertEquals(2L, seq1.getSequence(sequenceName).next());
    Assert.assertEquals(3L, seq1.getSequence(sequenceName).next());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[1]);
    Assert.assertEquals(0L, seq2.getSequence(sequenceName).reset());

    ODatabaseRecordThreadLocal.INSTANCE.set(dbs[0]);
    Assert.assertEquals(0L, seq1.getSequence(sequenceName).current());

    if (RUN_PARALLEL_SYNC_TEST) {
      executeParallelSyncTest(dbs, sequenceName, SEQUENCE_TYPE.ORDERED);
    }
  }

  private void executeParallelSyncTest(final ODatabaseDocumentTx[] dbs, final String sequenceName, final SEQUENCE_TYPE sequenceType)
      throws Exception {
    // Run a function retrieving SEQ_RUN_COUNT numbers, in parallel,
    // to make sure they are distinct.
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    List<Callable<List<Long>>> callables = new ArrayList<Callable<List<Long>>>();
    for (int i = 0; i < DB_COUNT; ++i) {
      final int id = i;
      callables.add(new Callable<List<Long>>() {
        @Override
        public List<Long> call() throws Exception {
          final ODatabaseDocumentTx db = dbs[id];
          ODatabaseRecordThreadLocal.INSTANCE.set(db);

          List<Long> res = new ArrayList<Long>(SEQ_RUN_COUNT);

          OSequence seq = db.getMetadata().getSequenceLibrary().getSequence(sequenceName);
          for (int j = 0; j < SEQ_RUN_COUNT; ++j) {
            try {
              long value = seq.next();
              res.add(value);
              Thread.sleep(new Random().nextInt(50));
            } catch (OConcurrentModificationException ex) {
              failures.incrementAndGet();
            }
          }

          return res;
        }
      });
    }
    List<Future<List<Long>>> results = pool.invokeAll(callables);
    pool.shutdown();
    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    // Both free and ordered are required to be unique; test it
    final int totalValues = SEQ_RUN_COUNT * DB_COUNT;
    Set<Long> set = new HashSet<Long>(totalValues);
    for (int i = 0; i < DB_COUNT; ++i) {
      List<Long> singleResults = results.get(i).get();
      set.addAll(singleResults);
    }
    long expectedSize = set.size() - failures.get();

    Assert.assertEquals("Distributed sequence of type " + sequenceType + " generates duplicate values", totalValues, expectedSize);
  }
}
