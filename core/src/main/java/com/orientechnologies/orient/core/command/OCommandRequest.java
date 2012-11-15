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

/**
 * Generic GOF command pattern implementation. Execute a command passing the optional arguments "iArgs" and returns an Object.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 */
public interface OCommandRequest {
  public <RET> RET execute(Object... iArgs);

  /**
   * Returns the limit of result set. -1 means no limits.
   * 
   */
  public int getLimit();

  /**
   * Sets the maximum items the command can returns. -1 means no limits.
   * 
   * @param iLimit
   *          -1 = no limit. 1 to N to limit the result set.
   * @return
   */
  public OCommandRequest setLimit(int iLimit);

  /**
   * Returns true if the command doesn't change the database, otherwise false.
   */
  public boolean isIdempotent();

  /**
   * Returns the fetch plan if any
   * 
   * @return Fetch plan as unique string or null if it was not defined.
   */
  public String getFetchPlan();

  /**
   * Set the fetch plan. The format is:
   * 
   * <pre>
   * &lt;field&gt;:&lt;depth-level&gt;*
   * </pre>
   * 
   * Where:
   * <ul>
   * <li><b>field</b> is the name of the field to specify the depth-level. <b>*</b> wildcard means any fields</li>
   * <li><b>depth-level</b> is the depth level to fetch. -1 means infinite, 0 means no fetch at all and 1-N the depth level value.</li>
   * </ul>
   * Uses the blank spaces to separate the fields strategies.<br/>
   * Example:
   * 
   * <pre>
   * children:-1 parent:0 sibling:3 *:0
   * </pre>
   * 
   * <br/>
   * 
   * @param iFetchPlan
   * @return
   */
  public <RET extends OCommandRequest> RET setFetchPlan(String iFetchPlan);
}
