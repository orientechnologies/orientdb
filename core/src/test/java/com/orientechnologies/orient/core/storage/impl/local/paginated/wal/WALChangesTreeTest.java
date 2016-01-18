package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

@Test
public class WALChangesTreeTest {
  public void testOneAdd() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 10, 20, 30 }, 10);

    byte[] result = new byte[] { 0 };

    tree.applyChanges(result, 12);

    Assert.assertEquals(new byte[] { 30 }, result);

    result = new byte[] { 0, 0 };

    tree.applyChanges(result, 9);

    Assert.assertEquals(new byte[] { 0, 10 }, result);

    result = new byte[] { 0, 0, 0, 0, 0 };
    tree.applyChanges(result, 9);

    Assert.assertEquals(result, new byte[] { 0, 10, 20, 30, 0 });
  }

  public void testOneAddDM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 10, 20, 30 }, 10);

    OByteBufferPool bufferPool = new OByteBufferPool(20);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 10, 3), new byte[] { 10, 20, 30 });
  }

  public void testAddOverlappedVersions() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 35, 30 }, 11);
    tree.add(new byte[] { 10, 20 }, 10);

    byte[] result = new byte[] { 0, 0, 0 };
    tree.applyChanges(result, 10);

    Assert.assertEquals(result, new byte[] { 10, 20, 30 });
  }

  public void testAddOverlappedVersionsDM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 35, 30 }, 11);
    tree.add(new byte[] { 10, 20 }, 10);

    OByteBufferPool bufferPool = new OByteBufferPool(20);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 10, 3), new byte[] { 10, 20, 30 });
  }

  public void testAddOverlappedVersionsTwo() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 11, 12, 13 }, 1);
    tree.add(new byte[] { 33, 34, 35, }, 3);
    tree.add(new byte[] { 22, 23, 24, 25, 26 }, 2);

    byte[] result = new byte[] { 0, 0, 0, 0 };
    tree.applyChanges(result, 2);

    Assert.assertEquals(result, new byte[] { 22, 23, 24, 25 });
  }

  public void testAddOverlappedVersionsTwoDM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 11, 12, 13 }, 1);
    tree.add(new byte[] { 33, 34, 35, }, 3);
    tree.add(new byte[] { 22, 23, 24, 25, 26 }, 2);

    OByteBufferPool bufferPool = new OByteBufferPool(20);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 1, 6), new byte[] { 11, 22, 23, 24, 25, 26 });
  }

  public void testInsertCaseThree() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 10 }, 10);
    tree.add(new byte[] { 5 }, 5);
    tree.add(new byte[] { 15 }, 15);
    tree.add(new byte[] { 2 }, 2);

    byte[] result = new byte[] { 0 };
    tree.applyChanges(result, 10);

    Assert.assertEquals(result, new byte[] { 10 });

    tree.applyChanges(result, 5);
    Assert.assertEquals(result, new byte[] { 5 });

    tree.applyChanges(result, 15);
    Assert.assertEquals(result, new byte[] { 15 });

    tree.applyChanges(result, 2);
    Assert.assertEquals(result, new byte[] { 2 });
  }

  public void testInsertCaseThreeDM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 10 }, 10);
    tree.add(new byte[] { 5 }, 5);
    tree.add(new byte[] { 15 }, 15);
    tree.add(new byte[] { 2 }, 2);

    OByteBufferPool bufferPool = new OByteBufferPool(20);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 10, 1), new byte[] { 10 });
    Assert.assertEquals(getByteArray(buffer, 5, 1), new byte[] { 5 });
    Assert.assertEquals(getByteArray(buffer, 15, 1), new byte[] { 15 });
    Assert.assertEquals(getByteArray(buffer, 2, 1), new byte[] { 2 });
  }

  public void testInsertCase4and5() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 50 }, 50);
    tree.add(new byte[] { 60 }, 60);
    tree.add(new byte[] { 40 }, 40);

    tree.add(new byte[] { 30 }, 30);
    tree.add(new byte[] { 35 }, 35);

    byte[] result = new byte[] { 0 };

    tree.applyChanges(result, 50);
    Assert.assertEquals(result, new byte[] { 50 });

    tree.applyChanges(result, 60);
    Assert.assertEquals(result, new byte[] { 60 });

    tree.applyChanges(result, 40);
    Assert.assertEquals(result, new byte[] { 40 });

    tree.applyChanges(result, 30);
    Assert.assertEquals(result, new byte[] { 30 });

    tree.applyChanges(result, 35);
    Assert.assertEquals(result, new byte[] { 35 });
  }

  public void testInsertCase4and5DM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    tree.add(new byte[] { 50 }, 50);
    tree.add(new byte[] { 60 }, 60);
    tree.add(new byte[] { 40 }, 40);

    tree.add(new byte[] { 30 }, 30);
    tree.add(new byte[] { 35 }, 35);

    OByteBufferPool bufferPool = new OByteBufferPool(80);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 50, 1), new byte[] { 50 });
    Assert.assertEquals(getByteArray(buffer, 60, 1), new byte[] { 60 });
    Assert.assertEquals(getByteArray(buffer, 40, 1), new byte[] { 40 });
    Assert.assertEquals(getByteArray(buffer, 30, 1), new byte[] { 30 });
    Assert.assertEquals(getByteArray(buffer, 35, 1), new byte[] { 35 });
  }

  public void testInsertRandom() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    final byte[] data = new byte[30];

    final long ts = System.currentTimeMillis();
    System.out.println("TestInsertRandom seed : " + ts);

    final Random rnd = new Random(ts);
    for (int i = 0; i < 100; i++) {
      final int start = rnd.nextInt(data.length) - 3;
      final int length = rnd.nextInt(3) + 4;

      int cend = start + length;
      if (cend > data.length)
        cend = data.length;

      int cstart = start;
      if (cstart < 0)
        cstart = 0;

      byte[] value = new byte[cend - cstart];
      rnd.nextBytes(value);

      System.arraycopy(value, 0, data, cstart, cend - cstart);
      tree.add(value, cstart);
    }

    byte[] result = new byte[30];
    tree.applyChanges(result, 0);
    Assert.assertEquals(result, data);

    for (int i = -5; i < result.length; i++) {
      int start = i;
      int end = start + 5;

      if (start < 0)
        start = 0;

      if (end > data.length)
        end = data.length;

      result = new byte[end - start];
      System.arraycopy(data, start, result, 0, end - start);

      byte[] cresult = new byte[result.length];
      tree.applyChanges(cresult, start);

      Assert.assertEquals(cresult, result);
    }
  }

  public void testInsertRandomDM() {
    final OWALChangesTree tree = new OWALChangesTree();
    tree.setDebug(true);

    final byte[] data = new byte[30];

    final long ts = System.currentTimeMillis();
    System.out.println("TestInsertRandomDM seed : " + ts);

    final Random rnd = new Random(ts);
    for (int i = 0; i < 100; i++) {
      final int start = rnd.nextInt(data.length) - 3;
      final int length = rnd.nextInt(3) + 4;

      int cend = start + length;
      if (cend > data.length)
        cend = data.length;

      int cstart = start;
      if (cstart < 0)
        cstart = 0;

      byte[] value = new byte[cend - cstart];
      rnd.nextBytes(value);

      System.arraycopy(value, 0, data, cstart, cend - cstart);
      tree.add(value, cstart);
    }

    OByteBufferPool bufferPool = new OByteBufferPool(30);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    tree.applyChanges(buffer);

    Assert.assertEquals(getByteArray(buffer, 0, 30), data);
  }

  private byte[] getByteArray(ByteBuffer buffer, int position, int len) {
    final byte[] result = new byte[len];
    buffer.position(position);
    buffer.get(result);

    return result;
  }

}
