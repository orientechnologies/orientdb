package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSBTreeRidBagConcurrencyMultiRidBag {
  public static final String URL = "plocal:target/testdb/OSBTreeRidBagConcurrencyMultiRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentHashMap<ORID, ConcurrentSkipListSet<ORID>> ridTreePerDocument =
      new ConcurrentHashMap<ORID, ConcurrentSkipListSet<ORID>>();
  private final AtomicReference<Long> lastClusterPosition = new AtomicReference<Long>();

  private final CountDownLatch latch = new CountDownLatch(1);

  private ExecutorService threadExecutor = Executors.newCachedThreadPool();
  private ScheduledExecutorService addDocExecutor = Executors.newScheduledThreadPool(5);

  private volatile boolean cont = true;

  private int linkbagCacheSize;
  private int evictionSize;

  private int topThreshold;
  private int bottomThreshold;
  private OrientDB ctx;

  @Before
  public void beforeMethod() {
    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);
    ctx = new OrientDB("embedded:./target/testdb/", OrientDBConfig.defaultConfig());
    if (ctx.exists("OSBTreeRidBagConcurrencyMultiRidBag")) {
      ctx.drop("OSBTreeRidBagConcurrencyMultiRidBag");
    }
    ctx.execute(
            "create database OSBTreeRidBagConcurrencyMultiRidBag plocal users(admin identified by"
                + " 'adminpwd' role admin)")
        .close();
    ODatabaseSession db = ctx.open("OSBTreeRidBagConcurrencyMultiRidBag", "admin", "adminpwd");
    db.createClass("WithRidbag");
    db.close();
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testConcurrency() throws Exception {
    ODatabaseSession db = ctx.open("OSBTreeRidBagConcurrencyMultiRidBag", "admin", "adminpwd");
    for (int i = 0; i < 100; i++) {
      ODocument document = new ODocument("WithRidbag");
      ORidBag ridBag = new ORidBag();
      document.field("ridBag", ridBag);

      db.save(document);

      ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<ORID>());
      lastClusterPosition.set(document.getIdentity().getClusterPosition());
    }

    final List<Future<?>> futures = new ArrayList<Future<?>>();

    Random random = new Random();
    for (int i = 0; i < 5; i++)
      addDocExecutor.scheduleAtFixedRate(
          new DocumentAdder(), random.nextInt(250), 250, TimeUnit.MILLISECONDS);

    for (int i = 0; i < 5; i++) futures.add(threadExecutor.submit(new RidAdder(i)));

    for (int i = 0; i < 5; i++) futures.add(threadExecutor.submit(new RidDeleter(i)));

    latch.countDown();

    Thread.sleep(30 * 60000);

    addDocExecutor.shutdown();
    addDocExecutor.awaitTermination(30, TimeUnit.SECONDS);

    Thread.sleep(30 * 60000);

    cont = false;

    for (Future<?> future : futures) future.get();

    long amountOfRids = 0;
    for (ORID rid : ridTreePerDocument.keySet()) {
      ODocument document = db.load(rid);
      document.setLazyLoad(false);

      final ConcurrentSkipListSet<ORID> ridTree = ridTreePerDocument.get(rid);

      final ORidBag ridBag = document.field("ridBag");

      for (OIdentifiable identifiable : ridBag)
        Assert.assertTrue(ridTree.remove(identifiable.getIdentity()));

      Assert.assertTrue(ridTree.isEmpty());
      amountOfRids += ridBag.size();
    }

    System.out.println(
        "Total  records added :  " + db.countClusterElements(db.getDefaultClusterId()));
    System.out.println("Total rids added : " + amountOfRids);
    ctx.drop("");
    ctx.close();
  }

  public final class DocumentAdder implements Runnable {
    @Override
    public void run() {

      try (ODatabaseSession db =
          ctx.open("OSBTreeRidBagConcurrencyMultiRidBag", "admin", "adminpwd")) {
        ODocument document = new ODocument("WithRidbag");
        ORidBag ridBag = new ORidBag();
        document.field("ridBag", ridBag);

        db.save(document);
        ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<ORID>());

        while (true) {
          final long position = lastClusterPosition.get();
          if (position < document.getIdentity().getClusterPosition()) {
            if (lastClusterPosition.compareAndSet(
                position, document.getIdentity().getClusterPosition())) break;
          } else break;
        }
      }
    }
  }

  public class RidAdder implements Callable<Void> {
    private final int id;

    public RidAdder(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      final Random random = new Random();
      long addedRecords = 0;
      int retries = 0;

      latch.await();
      try (ODatabaseSession db =
          ctx.open("OSBTreeRidBagConcurrencyMultiRidBag", "admin", "adminpwd")) {
        while (cont) {
          List<ORID> ridsToAdd = new ArrayList<ORID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new ORecordId(0, positionCounter.incrementAndGet()));
          }

          final int defaultClusterId = db.getClass("WithRidbag").getDefaultClusterId();

          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final ORID orid = new ORecordId(defaultClusterId, position);

          while (true) {
            ODocument document = db.load(orid);
            document.setLazyLoad(false);

            ORidBag ridBag = document.field("ridBag");
            for (ORID rid : ridsToAdd) ridBag.add(rid);

            try {
              db.save(document);
            } catch (OConcurrentModificationException e) {
              retries++;
              continue;
            }

            break;
          }

          final ConcurrentSkipListSet<ORID> ridTree = ridTreePerDocument.get(orid);
          ridTree.addAll(ridsToAdd);
          addedRecords += ridsToAdd.size();
        }
      }

      System.out.println(
          RidAdder.class.getSimpleName()
              + ":"
              + id
              + "-"
              + addedRecords
              + " were added. retries : "
              + retries);
      return null;
    }
  }

  public class RidDeleter implements Callable<Void> {
    private final int id;

    public RidDeleter(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      final Random random = new Random();
      long deletedRecords = 0;
      int retries = 0;

      latch.await();
      try (ODatabaseSession db =
          ctx.open("OSBTreeRidBagConcurrencyMultiRidBag", "admin", "adminpwd")) {
        final int defaultClusterId = db.getClass("WithRidbag").getDefaultClusterId();
        while (cont) {
          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final ORID orid = new ORecordId(defaultClusterId, position);

          while (true) {
            ODocument document = db.load(orid);
            document.setLazyLoad(false);
            ORidBag ridBag = document.field("ridBag");
            Iterator<OIdentifiable> iterator = ridBag.iterator();

            List<ORID> ridsToDelete = new ArrayList<ORID>();
            int counter = 0;
            while (iterator.hasNext()) {
              OIdentifiable identifiable = iterator.next();
              if (random.nextBoolean()) {
                iterator.remove();
                counter++;
                ridsToDelete.add(identifiable.getIdentity());
              }

              if (counter >= 5) break;
            }

            try {
              db.save(document);
            } catch (OConcurrentModificationException e) {
              retries++;
              continue;
            }

            final ConcurrentSkipListSet<ORID> ridTree = ridTreePerDocument.get(orid);
            ridTree.removeAll(ridsToDelete);

            deletedRecords += ridsToDelete.size();
            break;
          }
        }
      }

      System.out.println(
          RidDeleter.class.getSimpleName()
              + ":"
              + id
              + "-"
              + deletedRecords
              + " were deleted. retries : "
              + retries);
      return null;
    }
  }
}
