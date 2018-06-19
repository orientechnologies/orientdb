package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/15/14
 */
public class OHashTableDirectoryV3Test {
  private static ODatabaseDocumentTx databaseDocumentTx;

  private static OHashTableDirectoryV3 directory;

  private OAtomicOperation atomicOperation;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("memory:" + OHashTableDirectoryV3Test.class.getSimpleName());
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    directory = new OHashTableDirectoryV3(".tsc", "hashTableDirectoryTest", "hashTableDirectoryTest",
        (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());

    final OAtomicOperation atomicOperation = startAtomicOperation(directory);
    directory.create(atomicOperation);
    endAtomicOperation();
  }

  private static OAtomicOperation startAtomicOperation(ODurableComponent component) throws IOException {
    return ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .startAtomicOperation(component, true);
  }

  private static void endAtomicOperation() throws IOException {
    ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager().endAtomicOperation(false, null);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    directory.delete();
    databaseDocumentTx.drop();
  }

  @Before
  public void beforeMethod() throws Exception {
    atomicOperation = startAtomicOperation(directory);
  }

  @After
  public void afterMethod() throws IOException {
    directory.clear(atomicOperation);
    endAtomicOperation();
  }

  @Test
  public void addFirstLevel() throws IOException {
    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++)
      level[i] = i;

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);

    Assert.assertEquals(index, 0);
    Assert.assertEquals(directory.getMaxLeftChildDepth(0), 2);
    Assert.assertEquals(directory.getMaxRightChildDepth(0), 3);
    Assert.assertEquals(directory.getNodeLocalDepth(0), 4);

    Assertions.assertThat(directory.getNode(0)).isEqualTo(level);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(0, i), i);
  }

  @Test
  public void changeFirstLevel() throws IOException {
    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++)
      level[i] = i;

    directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);

    for (int i = 0; i < level.length; i++)
      directory.setNodePointer(0, i, i + 100, atomicOperation);

    directory.setMaxLeftChildDepth(0, (byte) 100, atomicOperation);
    directory.setMaxRightChildDepth(0, (byte) 101, atomicOperation);
    directory.setNodeLocalDepth(0, (byte) 102, atomicOperation);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(0, i), i + 100);

    Assert.assertEquals(directory.getMaxLeftChildDepth(0), 100);
    Assert.assertEquals(directory.getMaxRightChildDepth(0), 101);
    Assert.assertEquals(directory.getNodeLocalDepth(0), 102);
  }

  @Test
  public void addThreeRemoveSecondAddNewAndChange() throws IOException {
    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++)
      level[i] = i;

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 0);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 100;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 1);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 200;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 2);

    directory.deleteNode(1, atomicOperation);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 300;

    index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    Assert.assertEquals(index, 1);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(1, i), i + 300);

    Assert.assertEquals(directory.getMaxLeftChildDepth(1), 5);
    Assert.assertEquals(directory.getMaxRightChildDepth(1), 6);
    Assert.assertEquals(directory.getNodeLocalDepth(1), 7);
  }

  @Test
  public void addRemoveChangeMix() throws IOException {
    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++)
      level[i] = i;

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 0);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 100;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 1);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 200;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 2);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 300;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 3);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 400;

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(index, 4);

    directory.deleteNode(1, atomicOperation);
    directory.deleteNode(3, atomicOperation);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 500;

    index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    Assert.assertEquals(index, 3);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 600;

    index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(index, 1);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 700;

    index = directory.addNewNode((byte) 11, (byte) 12, (byte) 13, level, atomicOperation);
    Assert.assertEquals(index, 5);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(3, i), i + 500);

    Assert.assertEquals(directory.getMaxLeftChildDepth(3), 5);
    Assert.assertEquals(directory.getMaxRightChildDepth(3), 6);
    Assert.assertEquals(directory.getNodeLocalDepth(3), 7);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(1, i), i + 600);

    Assert.assertEquals(directory.getMaxLeftChildDepth(1), 8);
    Assert.assertEquals(directory.getMaxRightChildDepth(1), 9);
    Assert.assertEquals(directory.getNodeLocalDepth(1), 10);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(5, i), i + 700);

    Assert.assertEquals(directory.getMaxLeftChildDepth(5), 11);
    Assert.assertEquals(directory.getMaxRightChildDepth(5), 12);
    Assert.assertEquals(directory.getNodeLocalDepth(5), 13);
  }

  @Test
  public void addThreePages() throws IOException {
    int firsIndex = -1;
    int secondIndex = -1;
    int thirdIndex = -1;

    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];

    for (int n = 0; n < ODirectoryFirstPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (firsIndex < 0)
        firsIndex = index;
    }

    for (int n = 0; n < ODirectoryPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (secondIndex < 0)
        secondIndex = index;
    }

    for (int n = 0; n < ODirectoryPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (thirdIndex < 0)
        thirdIndex = index;
    }

    Assert.assertEquals(firsIndex, 0);
    Assert.assertEquals(secondIndex, ODirectoryFirstPageV3.NODES_PER_PAGE);
    Assert.assertEquals(thirdIndex, ODirectoryFirstPageV3.NODES_PER_PAGE + ODirectoryPageV3.NODES_PER_PAGE);

    directory.deleteNode(secondIndex, atomicOperation);
    directory.deleteNode(firsIndex, atomicOperation);
    directory.deleteNode(thirdIndex, atomicOperation);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 1000;

    int index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(index, thirdIndex);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 2000;

    index = directory.addNewNode((byte) 11, (byte) 12, (byte) 13, level, atomicOperation);
    Assert.assertEquals(index, firsIndex);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 3000;

    index = directory.addNewNode((byte) 14, (byte) 15, (byte) 16, level, atomicOperation);
    Assert.assertEquals(index, secondIndex);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 4000;

    index = directory.addNewNode((byte) 17, (byte) 18, (byte) 19, level, atomicOperation);
    Assert.assertEquals(index, ODirectoryFirstPageV3.NODES_PER_PAGE + 2 * ODirectoryPageV3.NODES_PER_PAGE);

    Assert.assertEquals(directory.getMaxLeftChildDepth(thirdIndex), 8);
    Assert.assertEquals(directory.getMaxRightChildDepth(thirdIndex), 9);
    Assert.assertEquals(directory.getNodeLocalDepth(thirdIndex), 10);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(thirdIndex, i), i + 1000);

    Assert.assertEquals(directory.getMaxLeftChildDepth(firsIndex), 11);
    Assert.assertEquals(directory.getMaxRightChildDepth(firsIndex), 12);
    Assert.assertEquals(directory.getNodeLocalDepth(firsIndex), 13);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(firsIndex, i), i + 2000);

    Assert.assertEquals(directory.getMaxLeftChildDepth(secondIndex), 14);
    Assert.assertEquals(directory.getMaxRightChildDepth(secondIndex), 15);
    Assert.assertEquals(directory.getNodeLocalDepth(secondIndex), 16);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(secondIndex, i), i + 3000);

    final int lastIndex = ODirectoryFirstPageV3.NODES_PER_PAGE + 2 * ODirectoryPageV3.NODES_PER_PAGE;

    Assert.assertEquals(directory.getMaxLeftChildDepth(lastIndex), 17);
    Assert.assertEquals(directory.getMaxRightChildDepth(lastIndex), 18);
    Assert.assertEquals(directory.getNodeLocalDepth(lastIndex), 19);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(lastIndex, i), i + 4000);
  }

  @Test
  public void changeLastNodeSecondPage() throws IOException {
    long[] level = new long[OLocalHashTableV3.MAX_LEVEL_SIZE];

    for (int n = 0; n < ODirectoryFirstPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    for (int n = 0; n < ODirectoryPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    for (int n = 0; n < ODirectoryPageV3.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++)
        level[i] = i + n * 100;

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    directory.deleteNode(ODirectoryFirstPageV3.NODES_PER_PAGE + ODirectoryPageV3.NODES_PER_PAGE - 1, atomicOperation);

    for (int i = 0; i < level.length; i++)
      level[i] = i + 1000;

    int index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(index, ODirectoryFirstPageV3.NODES_PER_PAGE + ODirectoryPageV3.NODES_PER_PAGE - 1);

    directory.setMaxLeftChildDepth(index - 1, (byte) 10, atomicOperation);
    directory.setMaxRightChildDepth(index - 1, (byte) 11, atomicOperation);
    directory.setNodeLocalDepth(index - 1, (byte) 12, atomicOperation);

    for (int i = 0; i < level.length; i++)
      directory.setNodePointer(index - 1, i, i + 2000, atomicOperation);

    directory.setMaxLeftChildDepth(index + 1, (byte) 13, atomicOperation);
    directory.setMaxRightChildDepth(index + 1, (byte) 14, atomicOperation);
    directory.setNodeLocalDepth(index + 1, (byte) 15, atomicOperation);

    for (int i = 0; i < level.length; i++)
      directory.setNodePointer(index + 1, i, i + 3000, atomicOperation);

    Assert.assertEquals(directory.getMaxLeftChildDepth(index - 1), 10);
    Assert.assertEquals(directory.getMaxRightChildDepth(index - 1), 11);
    Assert.assertEquals(directory.getNodeLocalDepth(index - 1), 12);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(index - 1, i), i + 2000);

    Assert.assertEquals(directory.getMaxLeftChildDepth(index), 8);
    Assert.assertEquals(directory.getMaxRightChildDepth(index), 9);
    Assert.assertEquals(directory.getNodeLocalDepth(index), 10);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(index, i), i + 1000);

    Assert.assertEquals(directory.getMaxLeftChildDepth(index + 1), 13);
    Assert.assertEquals(directory.getMaxRightChildDepth(index + 1), 14);
    Assert.assertEquals(directory.getNodeLocalDepth(index + 1), 15);

    for (int i = 0; i < level.length; i++)
      Assert.assertEquals(directory.getNodePointer(index + 1, i), i + 3000);
  }
}
