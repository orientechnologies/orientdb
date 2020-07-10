package com.orientechnologies.agent.services.metrics.reporters;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Created by Enrico Risa on 16/07/2018. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Reporters {

  public JMXReporter jmx = new JMXReporter();
  public ConsoleReporterConfig console = new ConsoleReporterConfig();
  public CSVReporter csv = new CSVReporter();
  public PrometheusReporter prometheus = new PrometheusReporter();

  public Reporters() {}
}
