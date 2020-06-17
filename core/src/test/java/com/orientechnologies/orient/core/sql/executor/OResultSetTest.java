package com.orientechnologies.orient.core.sql.executor;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 04/11/16. */
public class OResultSetTest {
  @Test
  public void testResultStream() {
    OInternalResultSet rs = new OInternalResultSet();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.stream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(45, result.get().intValue());
  }

  @Test
  public void testResultEmptyVertexStream() {
    OInternalResultSet rs = new OInternalResultSet();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testResultEdgeVertexStream() {
    OInternalResultSet rs = new OInternalResultSet();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }
}
