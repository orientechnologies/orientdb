package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/19/14
 */
@Test
public class LocalMTCreateDocumentSpeedTest {
  private static final Random random = new Random();
  private ODatabaseDocumentTx database;
  private Date date = new Date();
  private CountDownLatch latch = new CountDownLatch(1);
  private List<Future> futures;
  private volatile boolean stop = false;
  private ExecutorService executorService = Executors.newCachedThreadPool();

  private final List<String> users = new ArrayList<String>();

  private final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  @BeforeClass
  public void init() {
    database = new ODatabaseDocumentTx(System.getProperty("url"));
    database.setSerializer(new ORecordSerializerBinary());

    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
    database.getMetadata().getSchema().createClass("Account");

    final OSecurity security = database.getMetadata().getSecurity();
    for (int i = 0; i < 100; i++) {
      users.add("user" + i);
      security.createUser("user" + i, "user" + i, "admin");
    }

    futures = new ArrayList<Future>();
    for (int i = 0; i < 1; i++) futures.add(executorService.submit(new Saver()));
  }

  public void cycle() throws Exception {
    latch.countDown();

    Thread.sleep(10 * 60 * 1000);
    stop = true;

    System.out.println("Stop insertion");
    long sum = 0;
    for (Future<Long> future : futures) sum += future.get();

    System.out.println("Speed : " + (sum / futures.size()) + " ns per document.");

    futures.clear();

    latch = new CountDownLatch(1);

    stop = false;
    System.out.println("Start reading");
    System.out.println("Doc count : " + database.countClass("Account"));

    for (int i = 0; i < 8; i++)
      futures.add(
          executorService.submit(
              new Reader(
                  database.countClass("Account"),
                  database.getMetadata().getSchema().getClass("Account").getDefaultClusterId())));

    latch.countDown();

    Thread.sleep(10 * 60 * 1000);

    stop = true;

    sum = 0;
    for (Future future : futures) sum += (Long) future.get();

    System.out.println("Speed : " + (sum / futures.size()) + " ns per document.");
  }

  @AfterClass
  public void deinit() {
    if (database != null) database.drop();
  }

  private final class Saver implements Callable<Long> {

    private Saver() {}

    @Override
    public Long call() throws Exception {
      Random random = new Random();
      latch.await();

      long counter = 0;
      long start = System.nanoTime();
      while (!stop) {

        final String user = users.get(random.nextInt(users.size()));

        final OPartitionedDatabasePool pool =
            poolFactory.get(System.getProperty("url"), user, user);
        final ODatabaseDocumentTx database = pool.acquire();

        ODocument record = new ODocument("Account");
        record.field("id", 1);
        record.field("name", "Luca");
        record.field("surname", "Garulli");
        record.field("birthDate", date);
        record.field("salary", 3000f);
        record.save();

        counter++;

        database.close();
      }
      long end = System.nanoTime();

      return ((end - start) / counter);
    }
  }

  private final class Reader implements Callable<Long> {

    private final int docCount;
    private final int clusterId;
    public volatile int size;

    public Reader(long docCount, int clusterId) {
      this.docCount = (int) docCount;
      this.clusterId = clusterId;
    }

    @Override
    public Long call() throws Exception {

      latch.await();
      final OPartitionedDatabasePool pool =
          poolFactory.get(System.getProperty("url"), "admin", "admin");
      final ODatabaseDocumentTx database = pool.acquire();

      long counter = 0;
      long start = System.nanoTime();
      while (!stop) {
        ODocument document = database.load(new ORecordId(clusterId, random.nextInt(docCount)));
        if (document != null) counter++;
      }
      long end = System.nanoTime();

      database.close();
      return ((end - start) / counter);
    }
  }
}
