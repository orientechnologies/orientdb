/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.profiler;

import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OService;

import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;

public interface OProfiler extends OService {

  enum METRIC_TYPE {
    CHRONO, COUNTER, STAT, SIZE, ENABLED, TEXT
  }

  METRIC_TYPE getType(String k);

  void updateCounter(String iStatName, String iDescription, long iPlus);

  void updateCounter(String iStatName, String iDescription, long iPlus, String iDictionary);

  long getCounter(String iStatName);

  String dump();

  String dumpCounters();

  OProfilerEntry getChrono(String string);

  long startChrono();

  @CheckReturnValue(when = When.NEVER)
  long stopChrono(String iName, String iDescription, long iStartTime);

  @CheckReturnValue(when = When.NEVER)
  long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary);

  @CheckReturnValue(when = When.NEVER)
  long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary, String payload);

  @CheckReturnValue(when = When.NEVER)
  long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary, String payload, String user);

  String dumpChronos();

  String[] getCountersAsString();

  String[] getChronosAsString();

  Date getLastReset();

  boolean isRecording();

  boolean startRecording();

  boolean stopRecording();

  void unregisterHookValue(String string);

  void configure(String string);

  void setAutoDump(int iNewValue);

  String metadataToJSON();

  Map<String, OPair<String, METRIC_TYPE>> getMetadata();

  void registerHookValue(String iName, String iDescription, METRIC_TYPE iType, OProfilerHookValue iHookValue);

  void registerHookValue(String iName, String iDescription, METRIC_TYPE iType, OProfilerHookValue iHookValue, String iMetadataName);

  String getSystemMetric(String iMetricName);

  String getProcessMetric(String iName);

  String getDatabaseMetric(String databaseName, String iName);

  String toJSON(String command, String iPar1);

  void resetRealtime(String iText);

  void dump(PrintStream out);

  int reportTip(String iMessage);

  void registerListener(OProfilerListener listener);

  void unregisterListener(OProfilerListener listener);

  String threadDump();

  boolean isEnterpriseEdition();
}
