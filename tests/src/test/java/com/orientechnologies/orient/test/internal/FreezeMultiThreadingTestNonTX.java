/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.test.internal;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class FreezeMultiThreadingTestNonTX {
  private static final int TRANSACTIONAL_CREATOR_THREAD_COUNT = 2;
  private static final int TRANSACTIONAL_UPDATER_THREAD_COUNT = 2;
  private static final int TRANSACTIONAL_DELETER_THREAD_COUNT = 2;

  private static final int CREATOR_THREAD_COUNT = 10;
  private static final int UPDATER_THREAD_COUNT = 10;
  private static final int DELETER_THREAD_COUNT = 10;

  private static final int DOCUMENT_COUNT = 1000;

  private static final int TRANSACTIONAL_DOCUMENT_COUNT = 1000;

  private static final String URL = "remote:localhost/FreezeMultiThreadingTestNonTX";

  private static final String STUDENT_CLASS_NAME = "Student";
  private static final String TRANSACTIONAL_WORD = "Transactional";

  private AtomicInteger createCounter = new AtomicInteger(0);
  private AtomicInteger deleteCounter = new AtomicInteger(0);

  private AtomicInteger transactionalCreateCounter = new AtomicInteger(0);
  private AtomicInteger transactionalDeleteCounter = new AtomicInteger(0);

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(
          CREATOR_THREAD_COUNT + UPDATER_THREAD_COUNT + DELETER_THREAD_COUNT + 1);
  private ConcurrentSkipListSet<Integer> deleted = new ConcurrentSkipListSet<Integer>();

  private ConcurrentSkipListSet<Integer> deletedInTransaction =
      new ConcurrentSkipListSet<Integer>();
  private CountDownLatch countDownLatch = new CountDownLatch(1);

  private class NonTransactionalAdder implements Callable<Void> {
    public Void call() throws Exception {
      Thread.currentThread().setName("Adder - " + Thread.currentThread().getId());
      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);
        countDownLatch.await();

        long value = createCounter.getAndIncrement();
        while (value < DOCUMENT_COUNT) {
          Thread.sleep(200);
          ODocument document = new ODocument(STUDENT_CLASS_NAME);
          document.field("counter", value);
          document.save();

          if (value % 10 == 0)
            System.out.println(Thread.currentThread() + " : document " + value + " added");

          value = createCounter.getAndIncrement();
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalAdder implements Callable<Void> {
    public Void call() throws Exception {
      Thread.currentThread().setName("TransactionalAdder - " + Thread.currentThread().getId());
      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);
        countDownLatch.await();

        long value = transactionalCreateCounter.getAndIncrement();
        while (value < TRANSACTIONAL_DOCUMENT_COUNT) {
          Thread.sleep(200);

          database.begin();
          ODocument document = new ODocument(TRANSACTIONAL_WORD + STUDENT_CLASS_NAME);
          document.field("counter", value);
          document.save();

          database.commit();
          if (value % 10 == 0)
            System.out.println(Thread.currentThread() + " : document " + value + " added");

          value = transactionalCreateCounter.getAndIncrement();
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class NonTransactionalUpdater implements Callable<Void> {

    private int updateCounter = 0;

    public Void call() throws Exception {
      Thread.currentThread().setName("Updater - " + Thread.currentThread().getId());
      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);

        countDownLatch.await();

        while (updateCounter < DOCUMENT_COUNT) {
          if (updateCounter > createCounter.get()) {
            continue;
          }

          List<ODocument> execute = null;
          if (updateCounter % 10 == 0)
            System.out.println(
                Thread.currentThread() + " : before search cycle(update)" + updateCounter);
          do {
            try {
              execute =
                  database
                      .command(
                          new OSQLSynchQuery<Object>(
                              "select * from " + STUDENT_CLASS_NAME + " where counter = ?"))
                      .execute(updateCounter);
            } catch (ORecordNotFoundException onfe) {
              System.out.println("Record not found for doc " + updateCounter);
            } catch (OException e) {
              if (e.getMessage().contains("Error during loading record with id")) {
                // ignore this because record deleted
                System.out.println(Thread.currentThread() + "exception record already deleted");
                continue;
              }

              if (e.getCause() instanceof ConcurrentModificationException) {
                System.out.println(Thread.currentThread() + "exception concurrent modification");
                // ignore this because record deleted
                continue;
              }

              throw e;
            }
          } while (!deleted.contains(updateCounter) && (execute == null || execute.isEmpty()));
          if (!deleted.contains(updateCounter)) {
            if (updateCounter % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle(update) " + updateCounter);

            ODocument document = execute.get(0);
            document.field("counter2", document.<Object>field("counter"));
            try {
              document.save();

              if (updateCounter % 10 == 0)
                System.out.println(
                    Thread.currentThread() + " : document " + updateCounter + " updated");

              updateCounter++;
            } catch (ORecordNotFoundException ornfe) {
              System.out.println(Thread.currentThread() + " exception record already deleted");
            } catch (OConcurrentModificationException e) {
              System.out.println(
                  Thread.currentThread()
                      + " : concurrent modification exception while updating! "
                      + updateCounter);
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
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
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalUpdater implements Callable<Void> {
    private int updateCounter = 0;

    public Void call() throws Exception {
      Thread.currentThread().setName("TransactionalUpdater - " + Thread.currentThread().getId());
      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);

        countDownLatch.await();

        while (updateCounter < TRANSACTIONAL_DOCUMENT_COUNT) {
          if (updateCounter > transactionalCreateCounter.get()) {
            continue;
          }
          List<ODocument> execute = null;
          if (updateCounter % 10 == 0)
            System.out.println(
                Thread.currentThread() + " : before search cycle(update)" + updateCounter);
          do {
            try {
              execute =
                  database
                      .command(
                          new OSQLSynchQuery<Object>(
                              "select * from "
                                  + TRANSACTIONAL_WORD
                                  + STUDENT_CLASS_NAME
                                  + " where counter = ?"))
                      .execute(updateCounter);
            } catch (ORecordNotFoundException onfe) {
              // ignore has been deleted
            } catch (OException e) {
              if (e.getMessage().contains("Error during loading record with id")) {
                // ignore this because record deleted
                System.out.println(Thread.currentThread() + "exception record already deleted");
                continue;
              }

              if (e.getCause() instanceof ConcurrentModificationException) {
                System.out.println(Thread.currentThread() + "exception concurrent modification");
                // ignore this because record deleted
                continue;
              }

              throw e;
            }
          } while (!deletedInTransaction.contains(updateCounter)
              && (execute == null || execute.isEmpty()));
          if (!deletedInTransaction.contains(updateCounter)) {
            if (updateCounter % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle(update) " + updateCounter);

            database.begin();

            ODocument document = execute.get(0);
            document.field("counter2", document.<Object>field("counter"));
            try {
              document.save();

              database.commit();
              if (updateCounter % 10 == 0)
                System.out.println(
                    Thread.currentThread() + " : document " + updateCounter + " updated");

              updateCounter++;
            } catch (ORecordNotFoundException ornfe) {
              System.out.println(Thread.currentThread() + " exception record already deleted");
            } catch (OConcurrentModificationException e) {
              System.out.println(
                  Thread.currentThread()
                      + " : concurrent modification exception while updating! "
                      + updateCounter);
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
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
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class NonTransactionalDeleter implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread().setName("Deleter - " + Thread.currentThread().getId());

      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);

        countDownLatch.await();

        int number = deleteCounter.getAndIncrement();
        while (number < DOCUMENT_COUNT) {
          // wait while necessary document will be created
          while (number > createCounter.get()) ;
          try {

            List<ODocument> execute;
            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : before search cycle (delete) " + number);
            do {
              execute =
                  database
                      .command(
                          new OSQLSynchQuery<Object>(
                              "select * from " + STUDENT_CLASS_NAME + " where counter2 = ?"))
                      .execute(number);
            } while (execute == null || execute.isEmpty());

            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle (delete)" + number);

            ODocument document = execute.get(0);
            document.delete();

            deleted.add(number);

            if (number % 10 == 0)
              System.out.println(Thread.currentThread() + " : document deleted " + number);

            number = deleteCounter.getAndIncrement();
          } catch (OConcurrentModificationException e) {
            System.out.println(Thread.currentThread() + " : exception while deleted " + number);
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class TransactionalDeleter implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread()
          .setName("TransactionalDeleterDeleter - " + Thread.currentThread().getId());

      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
      database.open("admin", "admin");
      try {
        ODatabaseRecordThreadLocal.instance().set(database);

        countDownLatch.await();

        int number = transactionalDeleteCounter.getAndIncrement();
        while (number < TRANSACTIONAL_DOCUMENT_COUNT) {
          // wait while necessary document will be created
          while (number > transactionalCreateCounter.get()) ;
          try {

            List<ODocument> execute;
            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : before search cycle (delete) " + number);
            do {
              execute =
                  database
                      .command(
                          new OSQLSynchQuery<Object>(
                              "select * from "
                                  + TRANSACTIONAL_WORD
                                  + STUDENT_CLASS_NAME
                                  + " where counter2 = ?"))
                      .execute(number);
            } while (execute == null || execute.isEmpty());

            if (number % 10 == 0)
              System.out.println(
                  Thread.currentThread() + " : after search cycle (delete)" + number);

            database.begin();

            ODocument document = execute.get(0);
            document.delete();

            database.commit();

            deletedInTransaction.add(number);

            if (number % 10 == 0)
              System.out.println(Thread.currentThread() + " : document deleted " + number);

            number = transactionalDeleteCounter.getAndIncrement();
          } catch (OConcurrentModificationException e) {
            System.out.println(Thread.currentThread() + " : exception while deleted " + number);
          }
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
        database.close();
      }
    }
  }

  private class Locker implements Callable<Void> {

    public Void call() throws Exception {
      Thread.currentThread().setName("Locker - " + Thread.currentThread().getId());
      ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);

      try {
        countDownLatch.await();
        while (createCounter.get() < DOCUMENT_COUNT) {
          ODatabaseHelper.freezeDatabase(database);
          database.open("admin", "admin");

          final List<ODocument> beforeNonTxDocuments =
              database.query(new OSQLSynchQuery<Object>("select from " + STUDENT_CLASS_NAME));
          final List<ODocument> beforeTxDocuments =
              database.query(
                  new OSQLSynchQuery<Object>(
                      "select from " + TRANSACTIONAL_WORD + STUDENT_CLASS_NAME));

          database.close();

          System.out.println(
              "Freeze DB - nonTx : "
                  + beforeNonTxDocuments.size()
                  + " Tx : "
                  + beforeTxDocuments.size());
          try {
            Thread.sleep(10000);
          } finally {
            database.open("admin", "admin");
            final List<ODocument> afterNonTxDocuments =
                database.query(new OSQLSynchQuery<Object>("select from " + STUDENT_CLASS_NAME));
            final List<ODocument> afterTxDocuments =
                database.query(
                    new OSQLSynchQuery<Object>(
                        "select from " + TRANSACTIONAL_WORD + STUDENT_CLASS_NAME));
            assertDocumentAreEquals(beforeNonTxDocuments, afterNonTxDocuments);
            assertDocumentAreEquals(beforeTxDocuments, afterTxDocuments);

            database.close();
            System.out.println(
                "Release DB - nonTx : "
                    + afterNonTxDocuments.size()
                    + " Tx : "
                    + afterTxDocuments.size());
            ODatabaseHelper.releaseDatabase(database);
          }
          Thread.sleep(10000);
        }
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Throwable e) {
        e.printStackTrace();
        return null;
      } finally {
        System.out.println(
            Thread.currentThread()
                + "************************CLOSE************************************");
      }
    }
  }

  @BeforeMethod
  public void setUp() throws Exception {
    OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.setValue(1000);
    // OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.setValue(50);

    System.out.println("Create db");
    // SETUP DB
    try {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(URL);

      System.out.println("Recreating database");
      if (ODatabaseHelper.existsDatabase(db, "plocal")) {
        db.setProperty("security", Boolean.FALSE);
        ODatabaseHelper.dropDatabase(db, URL, "plocal");
      }
      ODatabaseHelper.createDatabase(db, URL);
      db.close();
    } catch (IOException ex) {
      System.out.println("Exception: " + ex);
      throw ex;
    }

    ODatabaseDocumentTx database = new ODatabaseDocumentTx(URL);
    database.open("admin", "admin");

    OClass student = database.getMetadata().getSchema().createClass(STUDENT_CLASS_NAME);

    student.createProperty("counter", OType.INTEGER);
    student.createProperty("counter2", OType.INTEGER);

    student.createIndex("index1", OClass.INDEX_TYPE.UNIQUE, "counter");
    student.createIndex("index2", OClass.INDEX_TYPE.NOTUNIQUE, "counter2");
    student.createIndex("index3", OClass.INDEX_TYPE.NOTUNIQUE, "counter", "counter2");

    OClass transactionalStudent =
        database.getMetadata().getSchema().createClass(TRANSACTIONAL_WORD + STUDENT_CLASS_NAME);

    transactionalStudent.createProperty("counter", OType.INTEGER);
    transactionalStudent.createProperty("counter2", OType.INTEGER);

    transactionalStudent.createIndex("index4", OClass.INDEX_TYPE.UNIQUE, "counter");
    transactionalStudent.createIndex("index5", OClass.INDEX_TYPE.NOTUNIQUE, "counter2");
    transactionalStudent.createIndex("index6", OClass.INDEX_TYPE.NOTUNIQUE, "counter", "counter2");

    System.out.println("*in before***********CLOSE************************************");
    database.close();
  }

  @Test
  public void test() throws Exception {
    Set<Future<Void>> threads =
        new HashSet<Future<Void>>(
            CREATOR_THREAD_COUNT + DELETER_THREAD_COUNT + UPDATER_THREAD_COUNT + 1);

    for (int i = 0; i < CREATOR_THREAD_COUNT; ++i) {
      NonTransactionalAdder thread = new NonTransactionalAdder();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < UPDATER_THREAD_COUNT; ++i) {
      NonTransactionalUpdater thread = new NonTransactionalUpdater();
      threads.add(executorService.submit(thread));
    }

    for (int i = 0; i < DELETER_THREAD_COUNT; ++i) {
      NonTransactionalDeleter thread = new NonTransactionalDeleter();
      threads.add(executorService.submit(thread));
    }

    // for (int i = 0; i < TRANSACTIONAL_CREATOR_THREAD_COUNT; ++i) {
    // TransactionalAdder thread = new TransactionalAdder();
    // threads.add(executorService.submit(thread));
    // }

    // for (int i = 0; i < TRANSACTIONAL_UPDATER_THREAD_COUNT; ++i) {
    // TransactionalUpdater thread = new TransactionalUpdater();
    // threads.add(executorService.submit(thread));
    // }
    //
    // for (int i = 0; i < TRANSACTIONAL_DELETER_THREAD_COUNT; ++i) {
    // TransactionalDeleter thread = new TransactionalDeleter();
    // threads.add(executorService.submit(thread));
    // }

    threads.add(executorService.submit(new Locker()));

    countDownLatch.countDown();
    for (Future<Void> future : threads) future.get();

    System.out.println("finish");
  }

  private void assertDocumentAreEquals(List<ODocument> firstDocs, List<ODocument> secondDocs) {
    if (firstDocs.size() != secondDocs.size()) Assert.fail();

    outer:
    for (final ODocument firstDoc : firstDocs) {
      for (final ODocument secondDoc : secondDocs) {
        if (firstDoc.equals(secondDoc)) {
          final ODatabaseDocumentInternal databaseRecord =
              ODatabaseRecordThreadLocal.instance().get();
          Assert.assertTrue(
              ODocumentHelper.hasSameContentOf(
                  firstDoc, databaseRecord, secondDoc, databaseRecord, null));
          continue outer;
        }
      }

      Assert.fail("Document " + firstDoc + " was changed during DB freeze");
    }
  }
}
