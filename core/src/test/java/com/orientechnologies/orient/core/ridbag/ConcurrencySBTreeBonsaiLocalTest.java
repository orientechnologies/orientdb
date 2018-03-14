package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBagFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;

public class ConcurrencySBTreeBonsaiLocalTest {

  @Test
  public void testName() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + ConcurrencySBTreeBonsaiLocalTest.class.getName());
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
      atomManager.startAtomicOperation(tree, false);
      for (int i = 1000; i < 2000; i++)
        tree.put(new ORecordId(10, i), 1);
      Future<?> ex = null;
      try {
        ex = exec.submit(new Runnable() {

          @Override
          public void run() {
            try {
              atomManager.startAtomicOperation(tree1, false);
              for (int i = 2000; i < 3000; i++)
                tree1.put(new ORecordId(10, i), 1);
              atomManager.endAtomicOperation(false, null);

            } catch (Exception e) {
              throw new RuntimeException(e);
            }

          }
        });
        ex.get(10, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Is supposed to go in deadlock correct that goes in timeout
      }

      atomManager.endAtomicOperation(false, null);
      ex.get();

      OSBTreeRidBag bag = OSBTreeRidBagFactory.getInstance().getOSBTreeRidBag();
      bag.setCollectionPointer(tree.getCollectionPointer());
      bag.setAutoConvertToRecord(false);
      Assert.assertEquals(tree.size(), 1000);
      for (OIdentifiable id : bag) {
        if (id.getIdentity().getClusterPosition() > 2000)
          Assert.fail("found a wrong rid in the ridbag");
      }
      OSBTreeRidBag secondBag = OSBTreeRidBagFactory.getInstance().getOSBTreeRidBag();
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
