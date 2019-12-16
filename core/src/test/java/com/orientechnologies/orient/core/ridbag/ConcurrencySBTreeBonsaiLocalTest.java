package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;

public class ConcurrencySBTreeBonsaiLocalTest {

  @Test
  public void testName() throws Exception {
    @SuppressWarnings({ "resource", "deprecation" })
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + ConcurrencySBTreeBonsaiLocalTest.class.getSimpleName());
    db.create();
    ExecutorService exec = Executors.newCachedThreadPool();
    try {
      OSBTreeCollectionManager coll = db.getSbTreeCollectionManager();
      OBonsaiCollectionPointer treePointer = coll.createSBTree(3, null);
      OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = (OSBTreeBonsaiLocal<OIdentifiable, Integer>) coll.loadSBTree(treePointer);

      OBonsaiCollectionPointer treePointer1 = coll.createSBTree(3, null);
      final OSBTreeBonsaiLocal<OIdentifiable, Integer> tree1 = (OSBTreeBonsaiLocal<OIdentifiable, Integer>) coll
          .loadSBTree(treePointer1);

      final OAtomicOperationsManager atomManager = ((OAbstractPaginatedStorage) db.getStorage()).getAtomicOperationsManager();
      final OAtomicOperation atomicOperation = atomManager.startAtomicOperation();
      for (int i = 1000; i < 2000; i++)
        tree.put(atomicOperation, new ORecordId(10, i), 1);
      Future<?> ex = null;
      try {
        ex = exec.submit(() -> {
          try {
            final OAtomicOperation atomicOperation1 = atomManager.startAtomicOperation();
            for (int i = 2000; i < 3000; i++)
              tree1.put(atomicOperation1, new ORecordId(10, i), 1);
            atomManager.endAtomicOperation(false);

          } catch (Exception e) {
            throw new RuntimeException(e);
          }

        });
        ex.get(10, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Is supposed to go in deadlock correct that goes in timeout
      }

      atomManager.endAtomicOperation(false);
      ex.get();

      OSBTreeRidBag bag = new OSBTreeRidBag();
      bag.setCollectionPointer(tree.getCollectionPointer());
      bag.setAutoConvertToRecord(false);
      Assert.assertEquals(tree.size(), 1000);
      for (OIdentifiable id : bag) {
        if (id.getIdentity().getClusterPosition() > 2000)
          Assert.fail("found a wrong rid in the ridbag");
      }
      OSBTreeRidBag secondBag = new OSBTreeRidBag();
      secondBag.setAutoConvertToRecord(false);
      secondBag.setCollectionPointer(tree1.getCollectionPointer());
      Assert.assertEquals(tree1.size(), 1000);
      for (OIdentifiable id : secondBag) {

        if (id.getIdentity().getClusterPosition() < 2000)
          Assert.fail("found a wrong rid in the ridbag");
      }

    } finally {
      exec.shutdown();
      db.drop();
    }
  }

}
