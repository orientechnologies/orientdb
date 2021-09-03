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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.db.ODatabase;
import java.util.Map;

/**
 * Basic interface for commands. Manages the context variables during execution.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OCommandContext {

  enum TIMEOUT_STRATEGY {
    RETURN,
    EXCEPTION
  }

  Object getVariable(String iName);

  Object getVariable(String iName, Object iDefaultValue);

  OCommandContext setVariable(String iName, Object iValue);

  OCommandContext incrementVariable(String getNeighbors);

  Map<String, Object> getVariables();

  OCommandContext getParent();

  OCommandContext setParent(OCommandContext iParentContext);

  OCommandContext setChild(OCommandContext context);

  /**
   * Updates a counter. Used to record metrics.
   *
   * @param iName Metric's name
   * @param iValue delta to add or subtract
   * @return
   */
  long updateMetric(String iName, long iValue);

  boolean isRecordingMetrics();

  OCommandContext setRecordingMetrics(boolean recordMetrics);

  void beginExecution(long timeoutMs, TIMEOUT_STRATEGY iStrategy);

  /**
   * Check if timeout is elapsed, if defined.
   *
   * @return false if it the timeout is elapsed and strategy is "return"
   * @throws OTimeoutException if the strategy is "exception" (default)
   */
  public boolean checkTimeout();

  public Map<Object, Object> getInputParameters();

  public void setInputParameters(Map<Object, Object> inputParameters);

  /** Creates a copy of execution context. */
  OCommandContext copy();

  /**
   * Merges a context with current one.
   *
   * @param iContext
   */
  void merge(OCommandContext iContext);

  ODatabase getDatabase();

  void declareScriptVariable(String varName);

  boolean isScriptVariableDeclared(String varName);
}
