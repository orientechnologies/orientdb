package com.orientechnologies.agent.profiler;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.agent.profiler.metrics.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 11/07/2018. */
public class OrientDBEnterpriseProfilerTest {

  @Test
  public void profilerCounterTest() {

    OMetricsRegistry profiler = new ODropWizardMetricsRegistry();

    OCounter counter = profiler.counter("test", "Test description");

    for (int i = 0; i < 10; i++) {
      counter.inc();
    }

    Assert.assertEquals(10, counter.getCount());

    Assert.assertEquals(1, profiler.getMetrics().size());

    Assert.assertEquals(counter, profiler.counter("test", ""));

    for (int i = 0; i < 10; i++) {
      counter.dec();
    }

    Assert.assertEquals(0, counter.getCount());

    counter.inc(10);

    Assert.assertEquals(10, counter.getCount());

    counter.dec(10);

    Assert.assertEquals(0, counter.getCount());
  }

  @Test
  public void profilerMeterTest() {

    OMetricsRegistry profiler = new ODropWizardMetricsRegistry();

    OMeter meter = profiler.meter("test", "Test description");

    for (int i = 0; i < 10; i++) {
      meter.mark();
    }

    Assert.assertEquals(10, meter.getCount());

    Assert.assertEquals(1, profiler.getMetrics().size());

    Assert.assertEquals(meter, profiler.meter("test", ""));

    meter.mark(10);

    Assert.assertEquals(20, meter.getCount());
  }

  @Test
  public void profilerGaugeTest() {

    OMetricsRegistry profiler = new ODropWizardMetricsRegistry();

    List<String> values = new ArrayList<>();

    OGauge<Integer> meter = profiler.gauge("test", "Test description", () -> values.size());

    Assert.assertEquals((int) meter.getValue(), values.size());

    values.add("Test");

    Assert.assertEquals((int) meter.getValue(), values.size());
  }

  @Test
  public void profilerHistogramTest() {

    OMetricsRegistry profiler = new ODropWizardMetricsRegistry();

    OHistogram histogram = profiler.histogram("test", "Test description");

    Assert.assertEquals(0, histogram.getCount());

    histogram.update(10);

    Assert.assertEquals(1, histogram.getCount());

    Assert.assertEquals(10, histogram.getSnapshot().getMax());
    Assert.assertEquals(10, histogram.getSnapshot().getMin());

    histogram.update(5);

    Assert.assertEquals(10, histogram.getSnapshot().getMax());
    Assert.assertEquals(5, histogram.getSnapshot().getMin());

    Assert.assertEquals(7.5, histogram.getSnapshot().getMean(), 0.1);
    Assert.assertEquals(10, histogram.getSnapshot().getMedian(), 0.1);
  }

  @Test
  public void profilerTimerTest() throws InterruptedException {

    OMetricsRegistry profiler = new ODropWizardMetricsRegistry();

    OTimer timer = profiler.timer("test", "Test description");

    Assert.assertEquals(0, timer.getCount());

    OTimer.OContext ctx = timer.time();

    Assert.assertEquals(0, timer.getCount());

    Thread.sleep(1000);

    long finalTime = ctx.stop();

    Assert.assertEquals(1, timer.getCount());

    assertThat(TimeUnit.NANOSECONDS.toMillis(finalTime)).isGreaterThanOrEqualTo(1000);
  }

  @Test
  public void testEnterpriseProfiler() {
    OEnterpriseProfiler profiler = new OEnterpriseProfiler();
    profiler.dump();
    profiler.startRecording();
    profiler.dumpHookValues();
    profiler.stopRecording();

    profiler.cpuUsageFallback();
  }
}
