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
import java.util.Map;

import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OService;

public interface OProfilerMBean extends OService {

  public enum METRIC_TYPE {
    CHRONO, COUNTER, STAT, SIZE, ENABLED, TEXT
  }

  public void updateCounter(String iStatName, String iDescription, long iPlus);

  public void updateCounter(String iStatName, String iDescription, long iPlus, String iDictionary);

  public long getCounter(String iStatName);

  public String dump();

  public String dumpCounters();

  public OProfilerEntry getChrono(String string);

  public long startChrono();

  public long stopChrono(String iName, String iDescription, long iStartTime);

  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary);

  public String dumpChronos();

  public String[] getCountersAsString();

  public String[] getChronosAsString();

  public Date getLastReset();

  public boolean isRecording();

  public boolean startRecording();

  public boolean stopRecording();

  public void unregisterHookValue(String string);

  public void configure(String string);

  public void setAutoDump(int iNewValue);

  public String metadataToJSON();

  public Map<String, OPair<String, METRIC_TYPE>> getMetadata();

  public void registerHookValue(final String iName, final String iDescription, final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue);

  public void registerHookValue(final String iName, final String iDescription, final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue, final String iMetadataName);

  public String getProcessMetric(String iName);

  public String getDatabaseMetric(String databaseName, String iName);

  public String toJSON(String command, final String iPar1);

  public void resetRealtime(final String iText);
}