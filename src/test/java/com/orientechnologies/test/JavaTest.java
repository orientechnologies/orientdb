package com.orientechnologies.test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

public class JavaTest {

  protected static AtomicLong countPerSeconds = new AtomicLong(0);
  protected static AtomicLong totalInserted   = new AtomicLong(0);
  protected static int        threadsNumber   = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) {
    int NUM_INSERTLOOPS = 256;

    NumberFormat formatter = new DecimalFormat("#0.00000");
    final long totalstart = System.currentTimeMillis();

    OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.setValue(0); // Turn off cache
    OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.setValue(0);
    OGlobalConfiguration.USE_WAL.setValue(false);
    OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(false);

    final OrientGraphFactory factory = new OrientGraphFactory("plocal:target/VLR", "admin", "admin");
    // final OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/VLR", "admin", "admin");
    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        System.out.printf("Total Vertex imported: [%d], Vertex per second: [%d] \n", totalInserted.get(), countPerSeconds.get());
        countPerSeconds.set(0);
      }
    }, 1000, 1000);
    try {
      OrientGraphNoTx graph = factory.getNoTx();
      graph.getRawGraph().set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, Runtime.getRuntime().availableProcessors());

      OrientVertexType ipv4 = graph.getVertexType("ipv4");
      if (ipv4 == null) {
        ipv4 = graph.createVertexType("ipv4");
        OrientVertexType.OrientVertexProperty decimal = ipv4.createProperty("Decimal", OType.LONG);
        // decimal.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
      }

      Thread[] threads = new Thread[threadsNumber];

      for (int i = 0; i < threadsNumber; i++) {
        threads[i] = new Thread(new IPAugmentRunnable(i, factory));
      }

      for (Thread thread : threads) {
        thread.start();
      }
      for (Thread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    } finally {
      // this also closes the OrientGraph instances created by the factory
      // Note that OrientGraphFactory does not implement Closeable
      factory.close();
    }

    long totalend = System.currentTimeMillis();
    System.out.println("** END ** Finished in " + formatter.format((totalend - totalstart) / 1000d) + " seconds");

  }

  protected static class IPAugmentRunnable implements Runnable {

    private OrientGraphFactory factory;
    private OrientGraphNoTx    graph;
    private int                number;

    public IPAugmentRunnable(int number, OrientGraphFactory factory) {
      this.number = number;
      this.factory = factory;
    }

    @Override
    public void run() {

      graph = factory.getNoTx();
      graph.declareIntent(new OIntentMassiveInsert().setEnableCache(false));
      NumberFormat formatter = new DecimalFormat("#0.00000");
      long start2 = System.currentTimeMillis();

      ODatabaseDocumentTx rawGraph = graph.getRawGraph();

      int bucket = (int) (Math.pow(2, 30) / threadsNumber);

      ODocument doc = new ODocument();
      for (int i = 0; i < bucket; i++) {

//        ODocument doc = new ODocument("ipv4");
        doc.setClassName("ipv4");
        long decimal = (i << threadsNumber) | number;
        doc.field("Decimal", decimal);
        rawGraph.save(doc, "ipv4_" + number);
        doc.reset();

        totalInserted.incrementAndGet();
        countPerSeconds.incrementAndGet();
      }
      long end2 = System.currentTimeMillis();
      graph.commit();
      System.out.println("====== Finished " + number + " in " + formatter.format((end2 - start2) / 1000d) + " seconds ======");
    }
  }
}
