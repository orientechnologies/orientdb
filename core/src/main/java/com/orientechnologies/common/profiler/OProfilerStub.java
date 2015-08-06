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

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OProfilerStub extends OAbstractProfiler {

  protected final ConcurrentMap<String, Long> counters = new ConcurrentHashMap<String, Long>();

  public OProfilerStub() {
  }

  public OProfilerStub(final OAbstractProfiler profiler) {
    super(profiler);
  }

  public void configure(final String iConfiguration) {
    if (iConfiguration == null || iConfiguration.length() == 0)
      return;

    if (isRecording())
      stopRecording();

    startRecording();
  }

  public boolean startRecording() {
    if (super.startRecording()) {
      counters.clear();
      tips.clear();
      return true;
    }
    return false;
  }

  public boolean stopRecording() {
    if (super.stopRecording()) {
      counters.clear();
      tips.clear();
      return true;
    }
    return false;
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
