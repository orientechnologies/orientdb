package com.orientechnologies.orient.test.internal;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** @author Artem Loginov (logart2007-at-gmail.com) */
@Test
public class LockMultithreadingTest {
  private static final int CREATOR_THREAD_COUNT = 10;
  private static final int UPDATER_THREAD_COUNT = 10;
  private static final int DELETOR_THREAD_COUNT = 10;
  private static final int DOCUMENT_COUNT = 10000000;
  private static final String URL = "plocal:megatest1";
  private ODatabaseDocumentTx db;

  private static final String STUDENT_CLASS_NAME = "Student";
  private static final String TRANSACTIONAL_WORD = "Transactional";

  private AtomicInteger createCounter = new AtomicInteger(0);
  private AtomicInteger deleteCounter = new AtomicInteger(0);

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(2 * DOCUMENT_COUNT + UPDATER_THREAD_COUNT);
  private ConcurrentSkipListSet<Integer> deleted = new ConcurrentSkipListSet<Integer>();
  private CountDownLatch countDownLatch = new CountDownLatch(1);

  private class NonTransactionalAdder implements Callable<Void> {
    public Void call() throws Exception {
      Thread.currentThread().setName("Adder - " + Thread.currentThread().getId());
      countDownLatch.await();
      ODatabaseRecordThreadLocal.instance().set(db);
      long value = createCounter.getAndIncrement();
      while (value < DOCUMENT_COUNT) {
        // because i like 7:)
        Thread.sleep(200);
        ODocument document = new ODocument(STUDENT_CLASS_NAME);
        document.field("counter", value);
        document.save();
        System.out.println(Thread.currentThread() + " : document " + value + " added");
        value = createCounter.getAndIncrement();
      }
      return null;
    }
  }

  private class NonTransactionalUpdater implements Callable<Void> {

    private int updateCounter = 0;

    public Void call() throws Exception {
      Thread.currentThread().setName("Updater - " + Thread.currentThread().getId());

      countDownLatch.await();

      while (updateCounter < DOCUMENT_COUNT) {
        if (updateCounter > createCounter.get()) {
          continue;
        }

        ODatabaseRecordThreadLocal.instance().set(db);
        List<ODocument> execute;
        System.out.println(
            Thread.currentThread() + " : before search cycle(update)" + updateCounter);
        do {
          execute =
              db.command(
                      new OSQLSynchQuery<Object>(
                          "select * from " + STUDENT_CLASS_NAME + " where counter = ?"))
                  .execute(updateCounter);

        } while (!deleted.contains(updateCounter) && (execute == null || execute.isEmpty()));
        if (!deleted.contains(updateCounter)) {
          System.out.println(
              Thread.currentThread() + " : after search cycle(update) " + updateCounter);
          ODocument document = execute.get(0);
          document.field("counter2", document.<Object>field("counter"));
          try {
            document.save();
            System.out.println(
                Thread.currentThread() + " : document " + updateCounter + " updated");
            updateCounter++;
          } catch (OConcurrentModificationException e) {
            System.out.println(
                Thread.currentThread()
                    + " : concurrent modification exception while updating! "
                    + updateCounter);
          }
        } else {
          System.out.println(
              Thread.currentThread()
                  + " : document "
                  + updateCounter
                  + " already deleted couldn't update!");
          updateCounter++;
        }
      }
      return null;
    }
  }

  private class NonTransactionalDeleter implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread().setName("Deleter - " + Thread.currentThread().getId());

      countDownLatch.await();

      int number = deleteCounter.getAndIncrement();
      while (number < DOCUMENT_COUNT) {
        // wait while necessary document will be created
        while (number > createCounter.get()) ;
        try {
          ODatabaseRecordThreadLocal.instance().set(db);

          List<ODocument> execute;
          System.out.println(Thread.currentThread() + " : before search cycle (delete) " + number);
          do {
            execute =
                db.command(
                        new OSQLSynchQuery<Object>(
                            "select * from " + STUDENT_CLASS_NAME + " where counter2 = ?"))
                    .execute(number);

          } while (execute == null || execute.isEmpty());
          System.out.println(Thread.currentThread() + " : after search cycle (delete)" + number);
          ODocument document = execute.get(0);
          document.delete();
          deleted.add(number);
          System.out.println(Thread.currentThread() + " : document deleted " + number);
          number = deleteCounter.getAndIncrement();
        } catch (OConcurrentModificationException e) {
          System.out.println(Thread.currentThread() + " : exception while deleted " + number);
        }
      }
      return null;
    }
  }

  @BeforeMethod
  public void setUp() throws Exception {
    System.out.println("Create db");
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);

    if (database.exists()) {
      database.open("admin", "admin").drop();
    }

    database.create();

    database.getMetadata().getSchema().createClass(STUDENT_CLASS_NAME);

    database.getMetadata().getSchema().createClass(TRANSACTIONAL_WORD + STUDENT_CLASS_NAME);

    this.db = database;
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {

    Set<Future> threads =
        new HashSet<Future>(CREATOR_THREAD_COUNT + DELETOR_THREAD_COUNT + UPDATER_THREAD_COUNT);

    for (int i = 0; i < CREATOR_THREAD_COUNT; ++i) {
      NonTransactionalAdder thread = new NonTransactionalAdder();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < UPDATER_THREAD_COUNT; ++i) {
      NonTransactionalUpdater thread = new NonTransactionalUpdater();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < DELETOR_THREAD_COUNT; ++i) {
      NonTransactionalDeleter thread = new NonTransactionalDeleter();
      threads.add(executorService.submit(thread));
    }
    //
    // for (int i = 0; i < 2; ++i) {
    // Thread thread = new Thread(new TransactionalAdder());
    // transactionalCreators.add(thread);
    // threads.add(thread);
    // }
    //
    // for (int i = 0; i < 2; ++i) {
    // Thread thread = new Thread(new TransactionalUpdater());
    // transactionalUpdaters.add(thread);
    // threads.add(thread);
    // }

    // for (int i = 0; i < 2; ++i) {
    // Thread thread = new Thread(new TransactionalDeleter());
    // transactionalDeleters.add(thread);
    // threads.add(thread);
    // }

    countDownLatch.countDown();
    for (Future future : threads) future.get();

    System.out.println("finish");
  }
}
