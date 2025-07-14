package com.orientechnologies.orient.core.transaction;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class OMimimumSequenceStatusTest {

  @Test
  public void basicMinimum() {
    OMinimumSequenceStatus status = new OMinimumSequenceStatus(2);
    ONodeId nodeOne = new ONodeId("one");
    ONodeId nodeTwo = new ONodeId("two");

    status.updateNode(nodeOne, new long[] {2, 2});
    status.updateNode(nodeTwo, new long[] {2, 2});

    assertArrayEquals(status.getMinimumSequence(), new long[] {2, 2});
  }

  @Test
  public void oneLagBehind() {
    OMinimumSequenceStatus status = new OMinimumSequenceStatus(2);
    ONodeId nodeOne = new ONodeId("one");
    ONodeId nodeTwo = new ONodeId("two");

    status.updateNode(nodeOne, new long[] {2, 2});
    status.updateNode(nodeTwo, new long[] {1, 1});

    assertArrayEquals(status.getMinimumSequence(), new long[] {1, 1});
  }

  @Test
  public void oneLagBehindOne() {
    OMinimumSequenceStatus status = new OMinimumSequenceStatus(2);
    ONodeId nodeOne = new ONodeId("one");
    ONodeId nodeTwo = new ONodeId("two");

    status.updateNode(nodeOne, new long[] {2, 2});
    status.updateNode(nodeTwo, new long[] {2, 1});

    assertArrayEquals(status.getMinimumSequence(), new long[] {2, 1});
  }
}
