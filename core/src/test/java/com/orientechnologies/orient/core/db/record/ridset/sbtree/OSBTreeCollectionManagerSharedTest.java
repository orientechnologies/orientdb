package com.orientechnologies.orient.core.db.record.ridset.sbtree;

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

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (OSBTreeBonsai<OIdentifiable, Integer> tree : createdTrees) {
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(tree.getRootBucketPointer());
      Assert.assertSame(cachedTree, tree);
      sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
    sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());

    Assert.assertEquals(sbTreeCollectionManager.size(), 6);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    for (int i = 4; i >= 0; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }
  }

  public void testEvictionTwoIsNotReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());
      Assert.assertSame(cachedTree, createdTree);

      if (i > 1)
        sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
    sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());

    Assert.assertEquals(sbTreeCollectionManager.size(), 8);

    for (int i = 9; i >= 5; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    for (int i = 4; i >= 2; i--) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }
  }

  public void testEvictionFiveIsNotReleased() {
    List<OSBTreeBonsai<OIdentifiable, Integer>> createdTrees = new ArrayList<OSBTreeBonsai<OIdentifiable, Integer>>();

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
      createdTrees.add(tree);
      sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());
    }

    Assert.assertEquals(sbTreeCollectionManager.size(), 10);

    for (int i = 0; i < 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());
      Assert.assertSame(cachedTree, createdTree);

      if (i > 4)
        sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.createSBTree();
    sbTreeCollectionManager.releaseSBTree(tree.getRootBucketPointer());

    Assert.assertEquals(sbTreeCollectionManager.size(), 11);

    for (int i = 0; i >= 10; i++) {
      OSBTreeBonsai<OIdentifiable, Integer> createdTree = createdTrees.get(i);
      OSBTreeBonsai<OIdentifiable, Integer> cachedTree = sbTreeCollectionManager.loadSBTree(createdTree.getRootBucketPointer());

      Assert.assertNotSame(cachedTree, createdTree);
      sbTreeCollectionManager.releaseSBTree(createdTree.getRootBucketPointer());
    }
  }
}
