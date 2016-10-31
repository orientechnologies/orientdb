package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OSequenceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 22/04/2016.
 */
public class OSequenceTest {

  private ODatabaseDocument db;

  @Rule
  public ExternalResource resource = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      db = new ODatabaseDocumentTx("memory:" + OSequenceTest.class.getName());
      db.create();
    }

    @Override
    protected void after() {
      db.drop();
    }

  };
  private OSequenceLibrary sequences;

  @Before
  public void setUp() throws Exception {

    sequences = db.getMetadata().getSequenceLibrary();

  }

  @Test
  public void shouldCreateSeqWithGivenAttribute() {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());

    assertThat(sequences.getSequenceCount()).isEqualTo(1);
    assertThat(sequences.getSequenceNames()).contains("MYSEQ");

    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.getSequenceType()).isEqualTo(OSequence.SEQUENCE_TYPE.ORDERED);
    assertThat(myseq.getMaxRetry()).isEqualTo(100);

  }

  @Test
  public void shouldGivesValuesOrdered() throws Exception {

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.current()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.current()).isEqualTo(2);

  }

  @Test
  public void shouldGivesValuesWithIncrement() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setDefaults().setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);

  }

  @Test
  public void shouldCache() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setDefaults().setCacheSize(100).setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq).isInstanceOf(OSequenceCached.class);
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);
    assertThat(myseq.current()).isEqualTo(60);
    assertThat(myseq.next()).isEqualTo(90);
    assertThat(myseq.current()).isEqualTo(90);
    assertThat(myseq.next()).isEqualTo(120);
    assertThat(myseq.current()).isEqualTo(120);

  }

  @Test(expected = OSequenceException.class)
  public void shouldThrowExceptionOnDuplicateSeqDefinition() throws Exception {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);

  }

  @Test
  public void shouldDropSequence() throws Exception {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.dropSequence("MYSEQ");
    assertThat(sequences.getSequenceCount()).isEqualTo(0);

    // IDEMPOTENT
    sequences.dropSequence("MYSEQ");
    assertThat(sequences.getSequenceCount()).isEqualTo(0);
  }

  @Test
  public void testCreateSequenceWithoutExplicitDefaults() throws Exception {
    // issue #6484
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
  }

  @Test
  public void shouldSequenceMTNoTx() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    OSequence mtSeq = sequences.createSequence("mtSeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    mtSeq.setMaxRetry(1000);
    final int count = 1000;
    final int threads = 2;
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger success = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; i++) {
      service.execute(new Runnable() {
        @Override
        public void run() {
          ODatabaseDocument databaseDocument = new ODatabaseDocumentTx("memory:" + OSequenceTest.class.getName());
          databaseDocument.open("admin", "admin");
          OSequence mtSeq1 = databaseDocument.getMetadata().getSequenceLibrary().getSequence("mtSeq");

          for (int j = 0; j < count / threads; j++) {
            try {
              mtSeq1.next();
              success.incrementAndGet();
            } catch (Exception e) {
              e.printStackTrace();
              errors.incrementAndGet();
            }
            latch.countDown();
          }
        }
      });
    }
    latch.await();

    assertThat(errors.get()).isEqualTo(0);
    assertThat(success.get()).isEqualTo(1000);
    mtSeq.reloadSequence();
    //    assertThat(mtSeq.getDocument().getVersion()).isEqualTo(1001);
    assertThat(mtSeq.current()).isEqualTo(1000);
  }

  @Test
  public void shouldSequenceMTTx() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    OSequence mtSeq = sequences.createSequence("mtSeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    final int count = 1000;
    final int threads = 2;
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger success = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; i++) {
      service.execute(new Runnable() {
        @Override
        public void run() {
          ODatabaseDocument databaseDocument = new ODatabaseDocumentTx("memory:" + OSequenceTest.class.getName());
          databaseDocument.open("admin", "admin");
          OSequence mtSeq1 = databaseDocument.getMetadata().getSequenceLibrary().getSequence("mtSeq");

          for (int j = 0; j < count / threads; j++) {
            for (int retry = 0; retry < 10; ++retry) {
              try {

                databaseDocument.begin();
                mtSeq1.next();
                databaseDocument.commit();
                success.incrementAndGet();
                break;

              } catch (OConcurrentModificationException e) {
                if (retry >= 10) {
                  e.printStackTrace();
                  errors.incrementAndGet();
                  break;
                }

                // RETRY
                try {
                  Thread.sleep(10 + new Random().nextInt(100));
                } catch (InterruptedException e1) {
                }
                mtSeq1.reloadSequence();
                continue;
              } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
              }
            }
            latch.countDown();
          }
        }
      });
    }
    latch.await();

    assertThat(errors.get()).isEqualTo(0);
    assertThat(success.get()).isEqualTo(1000);
    mtSeq.reloadSequence();
    assertThat(mtSeq.getDocument().getVersion()).isEqualTo(1001);
    assertThat(mtSeq.current()).isEqualTo(1000);
  }
}
