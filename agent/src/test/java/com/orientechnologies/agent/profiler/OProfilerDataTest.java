package com.orientechnologies.agent.profiler;

import org.junit.Assert;
import org.junit.Test;

public class OProfilerDataTest {

  @Test
  public void testClear(){
    OProfilerData data = new OProfilerData();
    data.updateCounter("foo", 1);
    data.updateStat("foo", 1);

    Assert.assertEquals(1, data.getCounter("foo"));
    data.clear("bar");
    Assert.assertEquals(1, data.getCounter("foo"));
    data.clear("fo");
    Assert.assertTrue(data.getCounter("foo") <= 0);
  }

  @Test
  public void testDump(){
    OProfilerData data = new OProfilerData();
    data.updateStat("foo", 123456);
    data.updateCounter("bar", 1);
    String result = data.dump();
    result.contains("foo");
    result.contains("123456");
  }
}
