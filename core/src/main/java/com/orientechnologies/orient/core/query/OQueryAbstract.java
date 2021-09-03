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
package com.orientechnologies.orient.core.query;

import com.orientechnologies.orient.core.command.OCommandRequestAbstract;
import com.orientechnologies.orient.core.fetch.OFetchHelper;

@SuppressWarnings("serial")
public abstract class OQueryAbstract<T extends Object> extends OCommandRequestAbstract
    implements OQuery<T> {
  public OQueryAbstract() {
    useCache = true;
  }

  @SuppressWarnings("unchecked")
  public <RET> RET execute(final Object... iArgs) {
    return (RET) run(iArgs);
  }

  /** Returns the current fetch plan. */
  public String getFetchPlan() {
    return fetchPlan;
  }

  /** Sets the fetch plan to use. */
  public OQueryAbstract setFetchPlan(final String fetchPlan) {
    OFetchHelper.checkFetchPlanValid(fetchPlan);
    if (fetchPlan != null && fetchPlan.length() == 0) {
      this.fetchPlan = null;
    } else {
      this.fetchPlan = fetchPlan;
    }
    return this;
  }

  /**
   * Resets the query removing the result set. Call this to reuse the Query object preventing a
   * pagination.
   */
  @Override
  public void reset() {}

  @Override
  public boolean isIdempotent() {
    return true;
  }
}
