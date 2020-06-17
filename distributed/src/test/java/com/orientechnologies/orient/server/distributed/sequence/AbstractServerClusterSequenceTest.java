package com.orientechnologies.orient.server.distributed.sequence;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public abstract class AbstractServerClusterSequenceTest extends AbstractServerClusterTest {
  private static final boolean RUN_PARALLEL_SYNC_TEST = true;
  private static final int SEQ_RUN_COUNT = 20;

  private static final int THREAD_COUNT = 2;
  private static final int THREAD_POOL_SIZE = 2;
  private static final int CACHE_SIZE = 3;

  private AtomicLong failures = new AtomicLong();

  @Override
  public String getDatabaseName() {
    return getClass().getSimpleName();
  }

  protected abstract String getDatabaseURL(ServerRun server);

  @Override
  public void executeTest() throws Exception {
    // Test two instances only;
    Assert.assertTrue("Test must run with at least 2 dbs", THREAD_COUNT >= 2);
    final ODatabaseDocument[] dbs = new ODatabaseDocument[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
      dbs[i] =
          serverInstance
              .get(i)
              .getServerInstance()
              .getContext()
              .open(getDatabaseName(), "admin", "admin");
    }

    executeOrderedSequenceTest(dbs, "seq0");

    executeCachedSequenceTest(dbs, "seq1");
  }

  private void executeCachedSequenceTest(final ODatabaseDocument[] dbs, final String sequenceName)
      throws ExecutionException, InterruptedException {
    // Assuming seq2.next() is called once after calling seq1.next() once, and cache size is
    // C, seq2.current() - seq1.current() = C

    OSequence seq1 =
        dbs[0]
            .getMetadata()
            .getSequenceLibrary()
            .createSequence(
                sequenceName,
                SEQUENCE_TYPE.CACHED,
                new OSequence.CreateParams().setDefaults().setCacheSize(CACHE_SIZE));
    dbs[1].activateOnCurrentThread();
    OSequence seq2 = dbs[1].getMetadata().getSequenceLibrary().getSequence(sequenceName);

    Assert.assertEquals(seq1.getName(), seq2.getName());
    Assert.assertEquals(seq1.getName(), sequenceName);

    Assert.assertEquals(seq1.getSequenceType(), seq2.getSequenceType());
    Assert.assertEquals(seq1.getSequenceType(), SEQUENCE_TYPE.CACHED);

    dbs[0].activateOnCurrentThread();
    long v1 = seq1.next();

    dbs[1].activateOnCurrentThread();
    long v2 = seq2.next();

    int protocolVersion = OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValue();
    if (protocolVersion != 2) {
      // OK this shouldn't be true when sequences are truly synchronized
      // but if sequences are treated as documents in database , this is true
      Assert.assertEquals((long) CACHE_SIZE, v2 - v1);
    } else {
      // in the last cycle sequences should be synchronized
      for (int i = 1; i < CACHE_SIZE; i++) {
        Assert.assertEquals(v1, v2 + (i - 1));
        dbs[0].activateOnCurrentThread();
        v1 = seq1.next();
        dbs[1].activateOnCurrentThread();
        v2 = seq2.current();
      }
      Assert.assertEquals(v1, v2);
    }
  }

  private void executeOrderedSequenceTest(final ODatabaseDocument[] dbs, final String sequenceName)
      throws Exception {
    OSequenceLibrary seq1 = dbs[0].getMetadata().getSequenceLibrary();
    OSequenceLibrary seq2 = dbs[1].getMetadata().getSequenceLibrary();

    dbs[0].activateOnCurrentThread();
    seq1.createSequence(sequenceName, SEQUENCE_TYPE.ORDERED, null);
    Assert.assertEquals(SEQUENCE_TYPE.ORDERED, seq1.getSequence(sequenceName).getSequenceType());

    dbs[1].activateOnCurrentThread();
    Assert.assertNotNull(
        "The sequence has not be propagated to the 2nd server", seq2.getSequence(sequenceName));

    dbs[0].activateOnCurrentThread();
    Assert.assertEquals(0L, seq1.getSequence(sequenceName).current());

    dbs[1].activateOnCurrentThread();
    Assert.assertEquals(0L, seq2.getSequence(sequenceName).current());
    Assert.assertEquals(1L, seq2.getSequence(sequenceName).next());

    dbs[0].activateOnCurrentThread();
    Assert.assertEquals(2L, seq1.getSequence(sequenceName).next());
    Assert.assertEquals(3L, seq1.getSequence(sequenceName).next());

    dbs[1].activateOnCurrentThread();
    Assert.assertEquals(0L, seq2.getSequence(sequenceName).reset());

    dbs[0].activateOnCurrentThread();
    Assert.assertEquals(0L, seq1.getSequence(sequenceName).current());

    if (RUN_PARALLEL_SYNC_TEST) {
      executeParallelSyncTest(dbs, sequenceName, SEQUENCE_TYPE.ORDERED);
    }
  }

  private void executeParallelSyncTest(
      final ODatabaseDocument[] dbs, final String sequenceName, final SEQUENCE_TYPE sequenceType)
      throws Exception {
    // Run a function retrieving SEQ_RUN_COUNT numbers, in parallel,
    // to make sure they are distinct.
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    List<Callable<List<Long>>> callables = new ArrayList<Callable<List<Long>>>();
    for (int i = 0; i < THREAD_COUNT; ++i) {
      final int id = i;
      callables.add(
          new Callable<List<Long>>() {
            @Override
            public List<Long> call() throws Exception {
              final ODatabaseDocument db = dbs[id];
              db.activateOnCurrentThread();

              List<Long> res = new ArrayList<Long>(SEQ_RUN_COUNT);

              OSequence seq = db.getMetadata().getSequenceLibrary().getSequence(sequenceName);

              // HIGH CONCURRENCY: INCREMENT THE RETRY
              seq.setMaxRetry(10000);

              for (int j = 0; j < SEQ_RUN_COUNT; ++j) {
                try {
                  long value = seq.next();

                  OLogManager.instance().info(this, "Thread %d step %d value %d", id, j, value);

                  res.add(value);
                } catch (OConcurrentModificationException ex) {
                  failures.incrementAndGet();
                  Thread.sleep(new Random().nextInt(100));
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
    final int expSize = SEQ_RUN_COUNT * THREAD_COUNT;
    Set<Long> set = new HashSet<Long>(expSize);
    for (int i = 0; i < THREAD_COUNT; ++i) {
      List<Long> singleResults = results.get(i).get();
      set.addAll(singleResults);
    }
    long totalValues = set.size() - failures.get();

    Assert.assertEquals(
        "Distributed sequence of type "
            + sequenceType
            + " generates duplicate values, failures="
            + failures.get(),
        expSize,
        totalValues);
  }
}
