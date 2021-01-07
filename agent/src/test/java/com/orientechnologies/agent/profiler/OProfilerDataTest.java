package com.orientechnologies.agent.profiler;

import org.junit.Assert;
import org.junit.Test;

public class OProfilerDataTest {

  @Test
  public void testBasics() {
    OProfilerData data = new OProfilerData();
    Assert.assertNull(data.getChrono(null));
    Assert.assertEquals(Long.MAX_VALUE, data.getRecordingTo());

    data.updateCounter(null, 10);
    Assert.assertEquals(-1L, data.getCounter(null));

    Assert.assertNull(data.getHookValue(null));
    Assert.assertNull(data.getHookValue("foobar"));

    Assert.assertNotNull(data.getCountersAsString());
    Assert.assertNotNull(data.getChronosAsString());
    Assert.assertNotNull(data.getCounters() );
    Assert.assertNotNull(data.getHooks());
    Assert.assertNotNull(data.getChronos());
    Assert.assertNotNull(data.getStats());

    Assert.assertNull(data.getStat(null));
    Assert.assertNull(data.getStat("foobar"));
    data.updateStat("foobar", 10L);
    Assert.assertEquals(10L, data.getStat("foobar").last);

  }



  @Test
  public void testClear() {
    OProfilerData data = new OProfilerData();
    data.updateCounter("foo", 1);
    data.updateStat("foo", 1);


    Assert.assertEquals(-1, data.getCounter("asdf"));
    Assert.assertEquals(1, data.getCounter("foo"));
    data.clear("bar");
    Assert.assertEquals(1, data.getCounter("foo"));
    data.clear("fo");
    Assert.assertTrue(data.getCounter("foo") <= 0);
  }

  @Test
  public void testDump() {
    OProfilerData data = new OProfilerData();
    data.updateStat("foo", 123456);
    data.updateCounter("bar", 1);
    String result = data.dump();
    Assert.assertTrue(result.contains("foo"));
    Assert.assertTrue(result.contains("123456"));
  }

  @Test
  public void testGetStatsAsString() {
    OProfilerData data = new OProfilerData();
    data.updateStat("foo", 123456);
    String[] result = data.getStatsAsString();
    result[0].contains("foo");
    result[0].contains("123456");

    data.getChronosAsString();
  }
}
