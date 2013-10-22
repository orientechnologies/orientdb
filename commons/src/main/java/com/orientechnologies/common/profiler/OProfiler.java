/*
 * Copyright 1999-2005 Luca Garulli (l.garulli--at-orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.profiler;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.util.OPair;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * 
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 * 
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OProfiler implements OProfilerMBean {

  public enum METRIC_TYPE {
    CHRONO, COUNTER, STAT, SIZE, ENABLED, TEXT
  }

  protected boolean               active   = false;
  private final Map<String, Long> counters = new HashMap<String, Long>();

  public interface OProfilerHookValue {
    public Object getValue();
  }

  public OProfiler() {
  }

  public void configure(final String iConfiguration) {
    if (iConfiguration == null || iConfiguration.length() == 0)
      return;

    if (isRecording())
      stopRecording();

    startRecording();
  }

  public void shutdown() {
    stopRecording();
  }

  public void startRecording() {
    if (active)
      return;

    active = true;
    counters.clear();
  }

  public void stopRecording() {
    if (!active)
      return;

    active = false;
    counters.clear();
  }

  public boolean isRecording() {
    return active;
  }

  public void updateCounter(final String iStatName, final String iDescription, final long iPlus) {
    updateCounter(iStatName, iDescription, iPlus, iStatName);
  }

  public void updateCounter(final String iStatName, final String iDescription, final long iPlus, final String iMetadata) {
    if (iStatName == null || !active)
      return;

    synchronized (counters) {
      final Long stat = counters.get(iStatName);
      final long oldValue = stat == null ? 0 : stat.longValue();
      counters.put(iStatName, new Long(oldValue + iPlus));
    }
  }

  public long getCounter(final String iStatName) {
    if (iStatName == null || !active)
      return -1;

    synchronized (counters) {
      final Long stat = counters.get(iStatName);
      if (stat == null)
        return -1;

      return stat.longValue();
    }
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void startup() {
  }

  @Override
  public String dump() {
    return null;
  }

  @Override
  public String dumpCounters() {
    return null;
  }

  @Override
  public OProfilerEntry getChrono(String string) {
    return null;
  }

  @Override
  public long startChrono() {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime) {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary) {
    return 0;
  }

  @Override
  public String dumpChronos() {
    return null;
  }

  @Override
  public String[] getCountersAsString() {
    return null;
  }

  @Override
  public String[] getChronosAsString() {
    return null;
  }

  @Override
  public Date getLastReset() {
    return null;
  }

  @Override
  public void unregisterHookValue(String string) {
  }

  @Override
  public void setAutoDump(int iNewValue) {
  }

  @Override
  public String metadataToJSON() {
    return null;
  }

  @Override
  public Map<String, OPair<String, METRIC_TYPE>> getMetadata() {
    return null;
  }

  @Override
  public void registerHookValue(String iName, String iDescription, METRIC_TYPE iType, OProfilerHookValue iHookValue) {
  }

  @Override
  public void registerHookValue(String iName, String iDescription, METRIC_TYPE iType, OProfilerHookValue iHookValue,
      String iMetadataName) {
  }

  public String getSystemMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("system.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  public String getProcessMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("process.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  public String getDatabaseMetric(final String iDatabaseName, final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("db.");
    buffer.append(iDatabaseName);
    buffer.append('.');
    buffer.append(iMetricName);
    return buffer.toString();
  }

  @Override
  public String toJSON(String command, final String iPar1) {
    return null;
  }
}
