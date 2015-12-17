/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.common.profiler;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PROFILER_MAXVALUES;

public class OProfilerStub extends OAbstractProfiler {

  protected  ConcurrentMap<String, Long>                    counters      = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity(
      PROFILER_MAXVALUES.getValueAsInteger()).build();
  private    ConcurrentLinkedHashMap<String, AtomicInteger> tips          = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity(
      PROFILER_MAXVALUES.getValueAsInteger()).build();
  private    ConcurrentLinkedHashMap<String, Long>          tipsTimestamp = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity(
      PROFILER_MAXVALUES.getValueAsInteger()).build();

  public OProfilerStub() {
  }

  public OProfilerStub(final OAbstractProfiler profiler) {
    super(profiler);
  }

  @Override
  protected void setTip(final String iMessage, final AtomicInteger counter) {
    tips.put(iMessage, counter);
    tipsTimestamp.put(iMessage, System.currentTimeMillis());
  }

  @Override
  protected AtomicInteger getTip(final String iMessage) {
    return tips.get(iMessage);
  }

  public void configure(final String iConfiguration) {
    if (iConfiguration == null || iConfiguration.length() == 0)
      return;

    if (isRecording())
      stopRecording();

    startRecording();
  }

  public boolean startRecording() {
    counters = new ConcurrentLinkedHashMap.Builder()
        .maximumWeightedCapacity(PROFILER_MAXVALUES.getValueAsInteger()).build();
    tips = new ConcurrentLinkedHashMap.Builder()
        .maximumWeightedCapacity(PROFILER_MAXVALUES.getValueAsInteger()).build();
    tipsTimestamp = new ConcurrentLinkedHashMap.Builder()
        .maximumWeightedCapacity(PROFILER_MAXVALUES.getValueAsInteger()).build();

    if (super.startRecording()) {
      counters.clear();
      return true;
    }
    return false;
  }

  public boolean stopRecording() {
    if (super.stopRecording()) {
      counters.clear();
      return true;
    }
    return false;
  }

  @Override
  public String dump() {
    if (recordingFrom < 0)
      return "<no recording>";

    final StringBuilder buffer = new StringBuilder(super.dump());

    if (tips.size() == 0)
      return buffer.toString();

    buffer.append("TIPS:");

    buffer.append(String.format("\n%100s +------------+", ""));
    buffer.append(String.format("\n%100s | Value      |", "Name"));
    buffer.append(String.format("\n%100s +------------+", ""));

    final List<String> names = new ArrayList<String>(tips.keySet());
    Collections.sort(names);

    for (String n : names) {
      final AtomicInteger v = tips.get(n);
      buffer.append(String.format("\n%-100s | %10d |", n, v.intValue()));
    }

    buffer.append(String.format("\n%100s +------------+", ""));
    return buffer.toString();
  }

  public void updateCounter(final String statName, final String description, final long plus, final String metadata) {
    if (statName == null || !isRecording())
      return;

    Long oldValue;
    Long newValue;
    do {
      oldValue = counters.get(statName);

      if (oldValue == null) {
        counters.putIfAbsent(statName, 0L);
        oldValue = counters.get(statName);
      }

      newValue = oldValue + plus;
    } while (!counters.replace(statName, oldValue, newValue));
  }

  public long getCounter(final String statName) {
    if (statName == null || !isRecording())
      return -1;

    final Long stat = counters.get(statName);
    if (stat == null)
      return -1;

    return stat;
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
  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary, String payload) {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary, String payload, String user) {
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
  public String metadataToJSON() {
    return null;
  }

  @Override
  public String toJSON(String command, final String iPar1) {
    return null;
  }

  @Override
  public void resetRealtime(String iText) {
  }

  /**
   * Updates the metric metadata.
   */
  protected void updateMetadata(final String iName, final String iDescription, final METRIC_TYPE iType) {
    if (iDescription != null && dictionary.putIfAbsent(iName, iDescription) == null)
      types.put(iName, iType);
  }
}
