package com.orientechnologies.common.profiler;

import com.orientechnologies.common.util.OPair;

import java.util.Date;
import java.util.Map;

public interface OProfilerMXBean {
  long getCounter(String iStatName);

  String dump();

  String dumpCounters();

  String dumpChronos();

  String[] getCountersAsString();

  String[] getChronosAsString();

  Date getLastReset();

  boolean isRecording();

  boolean startRecording();

  boolean stopRecording();

  void setAutoDump(int iNewValue);

  String metadataToJSON();

  String getSystemMetric(String iMetricName);

  String getProcessMetric(String iName);

  String getDatabaseMetric(String databaseName, String iName);

  void resetRealtime(String iText);
}
