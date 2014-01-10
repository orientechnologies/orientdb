package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;

@Test
public class OSBTreeCollectionManagerSharedTest {
  private String                         buildDirectory;
  private ODatabaseDocumentTx            databaseDocumentTx;
  private OSBTreeCollectionManagerShared sbTreeCollectionManager;

  @BeforeClass
  public void setUp() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/localSBTreeCompositeKeyTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();
  }

  @BeforeMethod
  public void beforeMethod() {
    sbTreeCollectionManager = new OSBTreeCollectionManagerShared(5, 10);
  }

  @AfterMethod
  public void afterMethod() {
    sbTreeCollectionManager.close();
  }

  @AfterClass
  public void tearDown() {
    databaseDocumentTx.drop();
  }

  public void testEvictionAllReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    final int clusterId = databaseDocumentTx.getDefaultClusterId();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(clusterId);
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

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(clusterId);
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
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(databaseDocumentTx.getDefaultClusterId());
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

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(databaseDocumentTx.getDefaultClusterId());
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
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(databaseDocumentTx.getDefaultClusterId());
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

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree(databaseDocumentTx.getDefaultClusterId());
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
