package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSBTreeCollectionManagerSharedTest extends DatabaseAbstractTest {
  private OSBTreeCollectionManagerShared sbTreeCollectionManager;
  private OAtomicOperationsManager atomicOperationsManager;

  @Before
  public void beforeMethod() {
    sbTreeCollectionManager =
        new OSBTreeCollectionManagerShared(
            5, 10, (OAbstractPaginatedStorage) database.getStorage());
    atomicOperationsManager =
        ((OAbstractPaginatedStorage) database.getStorage()).getAtomicOperationsManager();
  }

  @After
  public void afterMethod() {
    sbTreeCollectionManager.close();
  }

  @Test
  public void testEvictionAllReleased() throws Exception {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees =
        new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    final int clusterId = database.getDefaultClusterId();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  sbTreeCollectionManager.createAndLoadTree(atomicOperation, clusterId));
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(
          new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (OSBTreeBonsai<OIdentifiable, Integer> tree : createdTrees) {
      OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, tree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                sbTreeCollectionManager.createAndLoadTree(atomicOperation, clusterId));
    sbTreeCollectionManager.releaseSBTree(
        new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 6);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 0; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  @Test
  public void testEvictionTwoIsNotReleased() throws Exception {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  sbTreeCollectionManager.createAndLoadTree(
                      atomicOperation, database.getDefaultClusterId()));
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(
          new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      final OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());

      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, createdTree);

      if (i > 1) sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                sbTreeCollectionManager.createAndLoadTree(
                    atomicOperation, database.getDefaultClusterId()));
    sbTreeCollectionManager.releaseSBTree(
        new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 8);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  @Test
  public void testEvictionFiveIsNotReleased() throws Exception {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree =
          atomicOperationsManager.calculateInsideAtomicOperation(
              null,
              atomicOperation ->
                  sbTreeCollectionManager.createAndLoadTree(
                      atomicOperation, database.getDefaultClusterId()));
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(
          new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer =
          new OBonsaiCollectionPointer(createdTree.getFileId(), createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree =
          sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, createdTree);

      if (i > 4) sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree =
        atomicOperationsManager.calculateInsideAtomicOperation(
            null,
            atomicOperation ->
                sbTreeCollectionManager.createAndLoadTree(
                    atomicOperation, database.getDefaultClusterId()));
    sbTreeCollectionManager.releaseSBTree(
        new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 11);
  }
}
