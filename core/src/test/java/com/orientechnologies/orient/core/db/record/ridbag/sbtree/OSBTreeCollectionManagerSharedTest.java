package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

@Test
public class OSBTreeCollectionManagerSharedTest extends DatabaseAbstractTest {
  private OSBTreeCollectionManagerShared sbTreeCollectionManager;

  @BeforeMethod
  public void beforeMethod() {
    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(5, 10, (OAbstractPaginatedStorage) database.getStorage());
  }

  @AfterMethod
  public void afterMethod() {
    sbTreeCollectionManager.close();
  }

  public void testEvictionAllReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    final int clusterId = database.getDefaultClusterId();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(clusterId);
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (OSBTreeBonsai<OIdentifiable, Integer> tree : createdTrees) {
      OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, tree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(clusterId);
    sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 6);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 0; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  public void testEvictionTwoIsNotReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(database.getDefaultClusterId());
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      final OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());

      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, createdTree);

      if (i > 1)
        sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(database.getDefaultClusterId());
    sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 8);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  public void testEvictionFiveIsNotReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(database.getDefaultClusterId());
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);
      Assert.assertSame(cachedTree, createdTree);

      if (i > 4)
        sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createAndLoadTree(database.getDefaultClusterId());
    sbTreeCollectionManager.releaseSBTree(new OBonsaiCollectionPointer(tree.getFileId(), tree.getRootBucketPointer()));

    Assert.assertEquals(sbTreeCollectionManager.size(), 11);

    for (int i = 0; i >= 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      final OBonsaiCollectionPointer collectionPointer = new OBonsaiCollectionPointer(createdTree.getFileId(),
          createdTree.getRootBucketPointer());
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(collectionPointer);

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }
}
