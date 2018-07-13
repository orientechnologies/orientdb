package com.orientechnologies.agent.profiler;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.orientechnologies.agent.profiler.metrics.dropwizard.*;
import com.orientechnologies.common.profiler.OrientDBProfiler;
import com.orientechnologies.common.profiler.metrics.*;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by Enrico Risa on 09/07/2018.
 */
public class OrientDBEnterpriseProfiler implements OrientDBProfiler {

  private final MetricRegistry                 registry         = new MetricRegistry();
  private       ConcurrentMap<String, OMetric> metrics          = new ConcurrentHashMap<>();
  private       ConsoleReporter                consoleReporter  = null;
  private       CsvReporter                    csvReporter      = null;
  private       JmxReporter                    jmxReporter      = null;
  private       GraphiteReporter               graphiteReporter = null;
  private ODocument config;

  public OrientDBEnterpriseProfiler() {
    this(new ODocument().field("enabled", true));
  }

  public OrientDBEnterpriseProfiler(ODocument config) {
    this.config = config;

    configureProfiler(config);
  }

  private void configureProfiler(ODocument config) {

    ODocument reporters = config.field("reporters");
    if (reporters == null) {
      reporters = new ODocument();
    }
    jmxReporter = configureJMXReporter(getORDefault(reporters.field("jmx")));
    consoleReporter = configureConsoleReporter(getORDefault(reporters.field("console")));
  }

  private ODocument getORDefault(ODocument config) {
    return config != null ? config : new ODocument().field("enabled", false);
  }

  private JmxReporter configureJMXReporter(ODocument jmxConfig) {

    Boolean enabled = Boolean.TRUE.equals(jmxConfig.field("enabled"));
    String domain = jmxConfig.field("domain");

    JmxReporter.Builder builder = JmxReporter.forRegistry(registry);

    if (domain != null) {
      builder.inDomain(domain);
    }
    JmxReporter jmxReporter = builder.build();

    if (enabled) {
      jmxReporter.start();

    }
    return jmxReporter;

  }

  private ConsoleReporter configureConsoleReporter(ODocument consoleConfig) {

    Boolean enabled = Boolean.TRUE.equals(consoleConfig.field("enabled"));
    Number interval = consoleConfig.field("interval");

    ConsoleReporter.Builder builder = ConsoleReporter.forRegistry(registry);

    ConsoleReporter jmxReporter = builder.build();

    if (enabled && interval != null) {
      jmxReporter.start(interval.longValue(), TimeUnit.MILLISECONDS);

    }
    return jmxReporter;

  }

  @Override

  public String name(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  @Override
  public String name(Class<?> klass, String... names) {
    return MetricRegistry.name(klass, names);
  }

  @Override
  public OCounter counter(String name, String description) {
    return registerOrGetMetric(name, (key) -> new DropWizardCounter(registry.counter(key), key, description));
  }

  @Override
  public OMeter meter(String name, String description) {
    return registerOrGetMetric(name, (k) -> new DropWizardMeter(registry.meter(k), k, description));
  }

  @Override
  public <T> OGauge<T> gauge(String name, String description, Supplier<T> valueFunction) {
    return registerOrGetMetric(name,
        (k) -> new DropWizardGauge<T>(registry.register(k, () -> valueFunction.get()), k, description));
  }

  @Override
  public OHistogram histogram(String name, String description) {
    return registerOrGetMetric(name, (k) -> new DropWizardHistogram(registry.histogram(k), k, description));
  }

  @Override
  public OTimer timer(String name, String description) {
    return registerOrGetMetric(name, (k) -> new DropWizardTimer(registry.timer(k), k, description));
  }

  @Override
  public Map<String, OMetric> getMetrics() {
    return Collections.unmodifiableMap(metrics);
  }

  private <T extends OMetric> T registerOrGetMetric(String name, Function<String, OMetric> metric) {
    return (T) metrics.computeIfAbsent(name, metric);
  }

}
