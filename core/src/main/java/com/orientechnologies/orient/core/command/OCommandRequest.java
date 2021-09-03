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

import com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY;

/**
 * Generic GOF command pattern implementation. Execute a command passing the optional arguments
 * "iArgs" and returns an Object.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OCommandRequest {
  <RET> RET execute(Object... iArgs);

  /**
   * This api is deprecated use sql keyword "LIMIT" instead
   *
   * <p>Returns the limit of result set. -1 means no limits.
   */
  int getLimit();

  /**
   * This api is deprecated use sql keyword "LIMIT" instead
   *
   * <p>Sets the maximum items the command can returns. -1 means no limits.
   *
   * @param iLimit -1 = no limit. 1 to N to limit the result set.
   * @return
   */
  @Deprecated
  OCommandRequest setLimit(int iLimit);

  /**
   * This api is deprecated use sql keyword "TIMEOUT" instead
   *
   * <p>Returns the command timeout. 0 means no timeout.
   *
   * @return
   */
  @Deprecated
  long getTimeoutTime();

  /**
   * This api is deprecated use sql keyword "TIMEOUT" instead
   *
   * <p>Returns the command timeout strategy between the defined ones.
   *
   * @return
   */
  @Deprecated
  TIMEOUT_STRATEGY getTimeoutStrategy();

  /**
   * This api is deprecated use sql keyword "TIMEOUT" instead
   *
   * <p>Sets the command timeout. When the command execution time is major than the timeout the
   * command returns
   *
   * @param timeout
   */
  @Deprecated
  void setTimeout(long timeout, TIMEOUT_STRATEGY strategy);

  /** Returns true if the command doesn't change the database, otherwise false. */
  boolean isIdempotent();

  /**
   * This api is deprecated use sql keyword "FETCHPLAN" instead
   *
   * <p>Returns the fetch plan if any
   *
   * @return Fetch plan as unique string or null if it was not defined.
   */
  @Deprecated
  String getFetchPlan();

  /**
   * This api is deprecated use sql keyword "FETCHPLAN" instead
   *
   * <p>Set the fetch plan. The format is:
   *
   * <pre>
   * &lt;field&gt;:&lt;depth-level&gt;*
   * </pre>
   *
   * Where:
   *
   * <ul>
   *   <li><b>field</b> is the name of the field to specify the depth-level. <b>*</b> wildcard means
   *       any fields
   *   <li><b>depth-level</b> is the depth level to fetch. -1 means infinite, 0 means no fetch at
   *       all and 1-N the depth level value.
   * </ul>
   *
   * Uses the blank spaces to separate the fields strategies.<br>
   * Example:
   *
   * <pre>
   * children:-1 parent:0 sibling:3 *:0
   * </pre>
   *
   * <br>
   *
   * @param iFetchPlan
   * @return
   */
  @Deprecated
  <RET extends OCommandRequest> RET setFetchPlan(String iFetchPlan);

  void setUseCache(boolean iUseCache);

  OCommandContext getContext();

  OCommandRequest setContext(final OCommandContext iContext);
}
