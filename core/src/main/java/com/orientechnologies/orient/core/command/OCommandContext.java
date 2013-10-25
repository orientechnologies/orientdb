/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.command;

import java.util.Map;

/**
 * Basic interface for commands. Manages the context variables during execution.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OCommandContext {
  public enum TIMEOUT_STRATEGY {
    RETURN, EXCEPTION
  }

  public Object getVariable(String iName);

  public Object getVariable(String iName, Object iDefaultValue);

  public OCommandContext setVariable(final String iName, final Object iValue);

  public Map<String, Object> getVariables();

  public OCommandContext getParent();

  public OCommandContext setParent(OCommandContext iParentContext);

  public OCommandContext setChild(OCommandContext context);

  /**
   * Updates a counter. Used to record metrics.
   * 
   * @param iName
   *          Metric's name
   * @param iValue
   *          delta to add or subtract
   * @return
   */
  public long updateMetric(String iName, long iValue);

  public boolean isRecordingMetrics();

  public OCommandContext setRecordingMetrics(boolean recordMetrics);

  public void beginExecution(long timeoutMs, TIMEOUT_STRATEGY iStrategy);

  public boolean checkTimeout();
}