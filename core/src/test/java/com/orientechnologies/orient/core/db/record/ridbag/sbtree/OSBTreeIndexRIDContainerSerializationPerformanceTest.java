package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeIndexRIDContainerSerializationPerformanceTest {

  public static final int                  CYCLE_COUNT        = 20000;
  private static final int                 WARMUP_CYCLE_COUNT = 30000;
  public static final ODirectMemoryPointer POINTER            = new ODirectMemoryPointer(2048l);

  public static void main(String[] args) throws InterruptedException {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/testdb/OSBTreeRIDSetTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    ODatabaseRecordThreadLocal.INSTANCE.set(db);

    Set<OIdentifiable> data = new HashSet<OIdentifiable>(8);

    data.add(new ORecordId("#77:12"));
    data.add(new ORecordId("#77:13"));
    data.add(new ORecordId("#77:14"));
    data.add(new ORecordId("#77:15"));
    data.add(new ORecordId("#77:16"));

    for (int i = 0; i < WARMUP_CYCLE_COUNT; i++) {
      cycle(data);
    }

    System.gc();
    Thread.sleep(1000);

    long time = System.currentTimeMillis();
    for (int i = 0; i < CYCLE_COUNT; i++) {
      cycle(data);
    }
    time = System.currentTimeMillis() - time;

    System.out.println("Time: " + time + "ms.");
    System.out.println("Throughput: " + (((double) CYCLE_COUNT) * 1000 / time) + " rec/sec.");
  }

  private static void cycle(Set<OIdentifiable> data) {
    final OIndexRIDContainer valueContainer = new OIndexRIDContainer("ValueContainerPerformanceTest");
    valueContainer.addAll(data);
    OStreamSerializerSBTreeIndexRIDContainer.INSTANCE.serializeInDirectMemory(valueContainer, POINTER, 0l);
  }
}
