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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.listener.OProgressListener;

import java.util.Map;
import java.util.Set;

/**
 * Generic GOF command pattern implementation.
 * 
 * @author Luca Garulli
 */
public interface OCommandExecutor {

  /**
   * Parse the request. Once parsed the command can be executed multiple times by using the execute() method.
   * 
   * @param iRequest
   *          Command request implementation.
   * 
   * @see #execute(Map<Object, Object>...)
   * @return
   */
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest);

  /**
   * Execute the requested command parsed previously.
   * 
   * @param iArgs
   *          Optional variable arguments to pass to the command.
   * 
   * @see #parse(OCommandRequest)
   * @return
   */
  public Object execute(final Map<Object, Object> iArgs);

  /**
   * Set the listener invoked while the command is executing.
   * 
   * @param progressListener
   *          OProgressListener implementation
   * @return
   */
  public <RET extends OCommandExecutor> RET setProgressListener(OProgressListener progressListener);

  public <RET extends OCommandExecutor> RET setLimit(int iLimit);

  public String getFetchPlan();

  public Map<Object, Object> getParameters();

  public OCommandContext getContext();

  public void setContext(OCommandContext context);

  /**
   * Returns true if the command doesn't change the database, otherwise false.
   */
  public boolean isIdempotent();

  /**
   * Returns the involved clusters.
   */
  public Set<String> getInvolvedClusters();

  /**
   * Returns the security operation type use to check about security.
   * 
   * @return
   */
  public int getSecurityOperationType();

  boolean involveSchema();
}
