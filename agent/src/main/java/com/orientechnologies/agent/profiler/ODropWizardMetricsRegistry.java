package com.orientechnologies.agent.profiler;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.orientechnologies.agent.profiler.metrics.*;
import com.orientechnologies.agent.profiler.metrics.dropwizard.*;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.agent.services.metrics.reporters.CSVReporter;
import com.orientechnologies.agent.services.metrics.reporters.ConsoleReporterConfig;
import com.orientechnologies.agent.services.metrics.reporters.JMXReporter;
import com.orientechnologies.common.log.OLogManager;

import java.io.File;
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
public class ODropWizardMetricsRegistry implements OMetricsRegistry {

  private final MetricRegistry                 registry         = new MetricRegistry();
  private       ConcurrentMap<String, OMetric> metrics          = new ConcurrentHashMap<>();
  private       ConsoleReporter                consoleReporter  = null;
  private       CsvReporter                    csvReporter      = null;
  private       JmxReporter                    jmxReporter      = null;
  private       GraphiteReporter               graphiteReporter = null;
  private OrientDBMetricsSettings settings;

  public ODropWizardMetricsRegistry() {
    this(new OrientDBMetricsSettings());
  }

  public ODropWizardMetricsRegistry(OrientDBMetricsSettings settings) {
    this.settings = settings;

    configureProfiler(settings);
  }

  private void configureProfiler(OrientDBMetricsSettings settings) {

    jmxReporter = configureJMXReporter(settings.reporters.jmx);
    consoleReporter = configureConsoleReporter(settings.reporters.console);
    csvReporter = configureCsvReporter(settings.reporters.csv);
  }

  private JmxReporter configureJMXReporter(JMXReporter jmxConfig) {

    JmxReporter.Builder builder = JmxReporter.forRegistry(registry);

    if (jmxConfig.domain != null) {
      builder.inDomain(jmxConfig.domain);
    }
    JmxReporter jmxReporter = builder.build();

    if (jmxConfig.enabled) {
      jmxReporter.start();

    }
    return jmxReporter;

  }

  private ConsoleReporter configureConsoleReporter(ConsoleReporterConfig consoleConfig) {

    Boolean enabled = consoleConfig.enabled;
    Number interval = consoleConfig.interval;

    ConsoleReporter.Builder builder = ConsoleReporter.forRegistry(registry);

    ConsoleReporter jmxReporter = builder.build();

    if (enabled && interval != null) {
      jmxReporter.start(interval.longValue(), TimeUnit.MILLISECONDS);

    }
    return jmxReporter;

  }

  private CsvReporter configureCsvReporter(CSVReporter csvConfig) {

    Boolean enabled = csvConfig.enabled;
    Number interval = csvConfig.interval;
    String directory = csvConfig.directory;

    CsvReporter.Builder builder = CsvReporter.forRegistry(registry);
    CsvReporter csvReporter = null;
    if (enabled && interval != null && directory != null) {
      File outputDir = new File(directory);
      if (!outputDir.exists()) {
        if (!outputDir.mkdirs()) {
          OLogManager.instance().warn(this, "Failed to create CSV metrics dir {}", outputDir);
        }
      }
      csvReporter = builder.build(outputDir);
      csvReporter.start(interval.longValue(), TimeUnit.MILLISECONDS);
    }
    return csvReporter;

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

  public boolean remove(String name) {
    metrics.remove(name);
    return registry.remove(name);
  }
}
