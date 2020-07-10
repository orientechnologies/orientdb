package com.orientechnologies.agent.profiler;

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.orientechnologies.agent.profiler.metrics.*;
import com.orientechnologies.agent.profiler.metrics.dropwizard.*;
import com.orientechnologies.agent.profiler.source.CSVAggregateReporter;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.agent.services.metrics.reporters.CSVReporter;
import com.orientechnologies.agent.services.metrics.reporters.ConsoleReporterConfig;
import com.orientechnologies.agent.services.metrics.reporters.JMXReporter;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** Created by Enrico Risa on 09/07/2018. */
public class ODropWizardMetricsRegistry implements OMetricsRegistry {

  private final MetricRegistry registry = new MetricRegistry();
  private ConcurrentMap<String, OMetric> metrics = new ConcurrentHashMap<>();
  private ConsoleReporter consoleReporter = null;
  private CsvReporter csvReporter = null;
  private JmxReporter jmxReporter = null;
  private CSVAggregateReporter csvAggregates = null;
  private OEnterpriseServer server;
  private OrientDBMetricsSettings settings;
  private Map<Class<? extends OMetric>, Function<String, Metric>> metricFactory = new HashMap<>();

  private transient ObjectMapper mapper;

  public ODropWizardMetricsRegistry() {
    this(null, new OrientDBMetricsSettings());
  }

  public ODropWizardMetricsRegistry(OEnterpriseServer server, OrientDBMetricsSettings settings) {
    this.server = server;
    this.settings = settings;
    configureProfiler(settings);

    initFactories(settings);

    configureMapper(settings);
  }

  private void configureMapper(OrientDBMetricsSettings settings) {
    final TimeUnit rateUnit = TimeUnit.SECONDS;
    final TimeUnit durationUnit = TimeUnit.SECONDS;
    final boolean showSamples = false;
    MetricFilter filter = MetricFilter.ALL;
    this.mapper =
        new ObjectMapper()
            .registerModule(new MetricsModule(rateUnit, durationUnit, showSamples, filter));
  }

  private void initFactories(OrientDBMetricsSettings settings) {
    metricFactory.put(GCMetric.class, (s) -> registry.register(s, new GarbageCollectorMetricSet()));
    metricFactory.put(
        ThreadsMetric.class,
        (s) -> registry.register(s, new CachedThreadStatesGaugeSet(10, TimeUnit.SECONDS)));
    metricFactory.put(MemoryMetric.class, (s) -> registry.register(s, new MemoryUsageGaugeSet()));
  }

  private void configureProfiler(OrientDBMetricsSettings settings) {

    jmxReporter = configureJMXReporter(settings.reporters.jmx);
    consoleReporter = configureConsoleReporter(settings.reporters.console);
    csvReporter = configureCsvReporter(settings.reporters.csv);

    csvAggregates = configureCsvAggregatesReporter(server, settings.reporters.csv);

    if (settings.reporters.prometheus.enabled) {
      CollectorRegistry.defaultRegistry.register(new DropwizardExports(registry));
    }
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
          OLogManager.instance()
              .warn(this, "Failed to create CSV Aggregates metrics dir {}", outputDir);
        }
      }

      builder.filter((name, metric) -> !name.matches("(?s)db.*.query.*"));

      csvReporter = builder.build(outputDir);
      csvReporter.start(interval.longValue(), TimeUnit.MILLISECONDS);
    }
    return csvReporter;
  }

  private CSVAggregateReporter configureCsvAggregatesReporter(
      OEnterpriseServer server, CSVReporter csvConfig) {

    Boolean enabled = csvConfig.enabled;
    Number interval = csvConfig.interval;
    String directory = csvConfig.directory;

    CSVAggregateReporter.Builder builder = CSVAggregateReporter.forRegistry(server, registry);
    CSVAggregateReporter csvReporter = null;
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
    return registerOrGetMetric(
        name, (key) -> new DropWizardCounter(registry.counter(key), key, description));
  }

  @Override
  public OMeter meter(String name, String description) {
    return meter(name, description, ":");
  }

  @Override
  public OMeter meter(String name, String description, String unitOfMeasure) {
    return registerOrGetMetric(
        name, (k) -> new DropWizardMeter(registry.meter(k), k, description, unitOfMeasure));
  }

  @Override
  public <T> OGauge<T> gauge(String name, String description, Supplier<T> valueFunction) {
    return gauge(name, description, "", valueFunction);
  }

  @Override
  public <T> OGauge<T> gauge(
      String name, String description, String unitOfMeasure, Supplier<T> valueFunction) {
    return registerOrGetMetric(
        name,
        (k) ->
            new DropWizardGauge<T>(
                registry.register(k, () -> valueFunction.get()), k, description, unitOfMeasure));
  }

  @Override
  public <T> OGauge<T> newGauge(String name, String description, Supplier<T> valueFunction) {
    return new DropWizardGauge<T>(valueFunction::get, name, description);
  }

  @Override
  public <T> OGauge<T> newGauge(
      String name, String description, String unitOfMeasure, Supplier<T> valueFunction) {
    return new DropWizardGauge<T>(valueFunction::get, name, description, unitOfMeasure);
  }

  @Override
  public OHistogram histogram(String name, String description) {
    return registerOrGetMetric(
        name, (k) -> new DropWizardHistogram(registry.histogram(k), k, description));
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

  @Override
  public <T extends OMetric> T register(String name, String description, Class<T> klass) {
    return registerOrGetMetric(
        name,
        (k) -> {
          Function<String, Metric> function = metricFactory.get(klass);
          if (function != null) {
            Metric apply = function.apply(k);
            if (apply instanceof MetricSet) {
              return new DropWizardGenericSet((MetricSet) apply, k, description);
            } else {
              return new DropWizardGeneric(apply, k, description);
            }
          } else {
            return null;
          }
        });
  }

  @Override
  public <T extends OMetric> T register(String name, T metric) {

    if (metric instanceof DropWizardGeneric) {
      Metric m = ((DropWizardGeneric) metric).getMetric();
      registry.register(name, m);
    }
    return registerOrGetMetric(name, (k) -> metric);
  }

  @Override
  public void registerAll(OMetricSet metricSet) {
    registerAll(null, metricSet);
  }

  @Override
  public void registerAll(String prefix, OMetricSet metricSet) {

    for (Map.Entry<String, OMetric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof OMetricSet) {
        registerAll(name(prefix, entry.getKey()), (OMetricSet) entry.getValue());
      } else {
        register(name(prefix, entry.getKey()), entry.getValue());
      }
    }
    metrics.putIfAbsent(prefix, metricSet);
  }

  public boolean remove(String name) {
    OMetric oMetric = metrics.remove(name);

    if (oMetric instanceof OMetricSet) {
      String prefix = ((OMetricSet) oMetric).prefix();
      ((OMetricSet) oMetric)
          .getMetrics()
          .forEach(
              (k, v) -> {
                remove(prefix + "." + k);
              });
    }
    return registry.remove(name);
  }

  @Override
  public SortedMap<String, OHistogram> getHistograms(BiFunction<String, OMetric, Boolean> filter) {
    return getMetrics(OHistogram.class, filter);
  }

  private <T extends OMetric> SortedMap<String, T> getMetrics(
      Class<T> klass, BiFunction<String, OMetric, Boolean> filter) {
    final TreeMap<String, T> timers = new TreeMap<>();
    for (Map.Entry<String, OMetric> entry : metrics.entrySet()) {
      if (klass.isInstance(entry.getValue()) && filter.apply(entry.getKey(), entry.getValue())) {
        timers.put(entry.getKey(), (T) entry.getValue());
      }
    }
    return Collections.unmodifiableSortedMap(timers);
  }

  @Override
  public void toJSON(OutputStream outputStream) throws IOException {

    ObjectWriter writer = mapper.writer();

    writer.writeValue(outputStream, registry);
  }

  public MetricRegistry getInternal() {
    return registry;
  }
}
