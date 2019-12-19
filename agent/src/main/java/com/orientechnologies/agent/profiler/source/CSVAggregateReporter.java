package com.orientechnologies.agent.profiler.source;

import com.codahale.metrics.*;
import com.opencsv.CSVWriter;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.OClientConnectionStats;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Enrico Risa on 25/10/2018.
 */
public class CSVAggregateReporter {

  private static final String DEFAULT_SEPARATOR = ",";
  private final OEnterpriseServer server;
  private final MetricRegistry registry;
  private final File directory;
  private final Locale locale;
  private final String separator;
  private final TimeUnit rateUnit;
  private final TimeUnit durationUnit;
  private final Clock clock;
  private final MetricFilter filter;
  private final ScheduledExecutorService executor;
  private final boolean shutdownExecutorOnStop;
  private final CsvFileProvider csvFileProvider;
  private ScheduledFuture<?> scheduledFuture;

  public static CSVAggregateReporter.Builder forRegistry(OEnterpriseServer server, MetricRegistry registry) {
    return new CSVAggregateReporter.Builder(server, registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private final OEnterpriseServer server;
    private Locale locale;
    private String separator;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private Clock clock;
    private MetricFilter filter;
    private ScheduledExecutorService executor;
    private boolean shutdownExecutorOnStop;
    private CsvFileProvider csvFileProvider;

    public Builder(OEnterpriseServer server, MetricRegistry registry) {
      this.registry = registry;
      this.server = server;
      this.locale = Locale.getDefault();
      this.separator = DEFAULT_SEPARATOR;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.clock = Clock.defaultClock();
      this.filter = MetricFilter.ALL;
      this.executor = Executors.newSingleThreadScheduledExecutor();
      this.shutdownExecutorOnStop = true;
      this.csvFileProvider = new FixedNameCsvFileProvider();

    }

    /**
     * Builds a {@link CsvReporter} with the given properties, writing {@code .csv} files to the given directory.
     *
     * @param directory the directory in which the {@code .csv} files will be created
     * @return a {@link CsvReporter}
     */
    public CSVAggregateReporter build(File directory) {
      return new CSVAggregateReporter(server, registry, directory, locale, separator, rateUnit, durationUnit, clock, filter,
              executor, shutdownExecutorOnStop, csvFileProvider);
    }
  }

  private CSVAggregateReporter(OEnterpriseServer server, MetricRegistry registry, File directory, Locale locale, String separator,
                               TimeUnit rateUnit, TimeUnit durationUnit, Clock clock, MetricFilter filter, ScheduledExecutorService executor,
                               boolean shutdownExecutorOnStop, CsvFileProvider csvFileProvider) {
    this.server = server;
    this.registry = registry;
    this.directory = directory;
    this.locale = locale;
    this.separator = separator;
    this.rateUnit = rateUnit;
    this.durationUnit = durationUnit;
    this.clock = clock;
    this.filter = filter;
    this.executor = executor;
    this.shutdownExecutorOnStop = shutdownExecutorOnStop;
    this.csvFileProvider = csvFileProvider;

  }

  public void start(long period, TimeUnit unit) {

    if (this.scheduledFuture != null) {
      if (this.scheduledFuture != null) {
        throw new IllegalArgumentException("Reporter already started");
      }
    }
    this.scheduledFuture = executor.scheduleAtFixedRate(() -> {
      report();
    }, period, period, unit);

  }

  public void report() {

    SortedMap<String, Histogram> histograms = registry.getHistograms((name, metric) -> name.matches("db.*.query.*"));

    final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

    List<List<Object>> collected = getQueryStats(histograms);
    report(timestamp, "db.queries", "database,language,query,count,min(millis),mean(millis),max(millis)", collected);

    List<List<Object>> runningQueries = getRunningQueries();
    report(timestamp, "db.runningQueries", "queryId,sessionId,database,user,language,query,startTime,elapsedTime(millis)",
            runningQueries);

    List<List<Object>> stats = getConnections();

    report(timestamp, "server.network.activeSessions",
            "connectionId,remoteAddress,database,user,totalRequests,commandInfo,commandDetail,lastCommandOn,lastCommandInfo,"
                    + "lastCommandDetail,lastExecutionTime,totalWorkingTime,activeQueries,connectedOn,protocol,sessionId,clientId,driver",
            stats);
  }

  private List<List<Object>> getQueryStats(SortedMap<String, Histogram> histograms) {
    return histograms.entrySet().stream().sorted((v1, v2) -> {
      Snapshot snapshot1 = v1.getValue().getSnapshot();
      Snapshot snapshot2 = v2.getValue().getSnapshot();
      return Double.compare(snapshot2.getMean(), snapshot1.getMean());
    }).map((e) -> {
      List<Object> value = new ArrayList<>();
      String key = e.getKey();
      Histogram h = e.getValue();
      Snapshot snapshot = h.getSnapshot();
      String statement = key.substring(key.indexOf(".query.") + 7);
      String language = statement.substring(0, statement.indexOf("."));
      String query = statement.substring(statement.indexOf(".") + 1);
      String db = key.substring(key.indexOf("db.") + 3, key.indexOf(".query."));
      value.add(db);
      value.add(language);
      value.add(query);
      value.add(h.getCount());
      value.add(snapshot.getMax());
      value.add(snapshot.getMean());
      value.add(snapshot.getMax());
      return value;
    }).collect(Collectors.toList());
  }

  private List<List<Object>> getRunningQueries() {
    return server.listQueries(Optional.empty()).stream().sorted((v1, v2) -> {
      Long l1 = v1.getProperty("elapsedTimeMillis");
      Long l2 = v2.getProperty("elapsedTimeMillis");
      return l2.compareTo(l1);
    }).map((r) -> {
      List<Object> value = new ArrayList<>();
      value.add(r.getProperty("queryId"));
      value.add(r.getProperty("sessionId"));
      value.add(r.getProperty("user"));
      value.add(r.getProperty("database"));
      value.add(r.getProperty("language"));
      value.add(r.getProperty("query"));
      value.add(r.getProperty("startTime"));
      value.add(r.getProperty("elapsedTimeMillis"));
      return value;
    }).collect(Collectors.toList());
  }

  private List<List<Object>> getConnections() {
    List<List<Object>> values = server.getConnections().stream().map((c) -> {

      List<Object> value = new ArrayList<>();

      final ONetworkProtocolData data = c.getData();
      final OClientConnectionStats stats = c.getStats();

      final DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final String lastCommandOn;
      final String connectedOn;
      synchronized (dateTimeFormat) {
        lastCommandOn = dateTimeFormat.format(new Date(stats.lastCommandReceived));
        connectedOn = dateTimeFormat.format(new Date(c.getSince()));
      }
      String lastDatabase;
      String lastUser;
      if (stats.lastDatabase != null && stats.lastUser != null) {
        lastDatabase = stats.lastDatabase;
        lastUser = stats.lastUser;
      } else {
        lastDatabase = data.lastDatabase;
        lastUser = data.lastUser;
      }

      value.add(c.getId());
      value.add(c.getProtocol().getChannel() != null ? c.getProtocol().getChannel().toString() : "Disconnected");
      value.add(lastDatabase != null ? lastDatabase : "-");
      value.add(lastUser != null ? lastUser : "-");
      value.add(stats.totalRequests);
      value.add(data.commandInfo);
      value.add(data.commandDetail);
      value.add(lastCommandOn);
      value.add(stats.lastCommandInfo);
      value.add(stats.lastCommandDetail);
      value.add(stats.lastCommandExecutionTime);
      value.add(stats.totalCommandExecutionTime);
      value.add(stats.activeQueries != null ? stats.activeQueries.size() : "-");
      value.add(connectedOn);
      value.add(c.getProtocol().getType());
      value.add(data.sessionId);
      value.add(data.clientId);

      final StringBuilder driver = new StringBuilder(128);
      if (data.driverName != null) {
        driver.append(data.driverName);
        driver.append(" v");
        driver.append(data.driverVersion);
        driver.append(" Protocol v");
        driver.append(data.protocolVersion);
      }
      value.add(driver.toString());
      return value;
    }).collect(Collectors.toList());
    return values;
  }

  public void stop() {
    if (shutdownExecutorOnStop) {
      executor.shutdown(); // Disable new tasks from being submitted
      try {
        // Wait a while for existing tasks to terminate
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
          executor.shutdownNow(); // Cancel currently executing tasks
          // Wait a while for tasks to respond to being cancelled
          if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            System.err.println(getClass().getSimpleName() + ": ScheduledExecutorService did not terminate");
          }
        }
      } catch (InterruptedException ie) {
        // (Re-)Cancel if current thread also interrupted
        executor.shutdownNow();
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    } else {
      // The external manager(like JEE container) responsible for lifecycle of executor
      synchronized (this) {
        if (this.scheduledFuture == null) {
          // was never started
          return;
        }
        if (this.scheduledFuture.isCancelled()) {
          // already cancelled
          return;
        }
        // just cancel the scheduledFuture and exit
        this.scheduledFuture.cancel(false);
      }
    }
  }

  private void report(long timestamp, String name, String header, List<List<Object>> values) {
    try {
      final File file = csvFileProvider.getFile(directory, name);
      final boolean fileAlreadyExists = file.exists();
      if (fileAlreadyExists || file.createNewFile()) {

        List<Object> v = Collections.singletonList(timestamp);

        CSVWriter writer = new CSVWriter(new FileWriter(file));
        try {
          writer.writeNext(("timestamp" + DEFAULT_SEPARATOR + header).split(DEFAULT_SEPARATOR));
          for (List<Object> value : values) {
            String[] val = Stream.concat(v.stream(), value.stream()).map((s) -> s != null ? s.toString() : "-")
                    .toArray(size -> new String[size]);
            writer.writeNext(val);
          }
        } finally {
          try {
            writer.close();
          } catch (IOException e) {
            OLogManager.instance().warn(this, "Failed to close resource " + writer);
          }
        }

      }
    } catch (IOException e) {
      OLogManager.instance().warn(this, "Error writing to {}", name, e);
    }
  }
}
