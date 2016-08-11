/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.event.metric;

import com.orientechnologies.agent.event.OEventExecutor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;

public abstract class OEventMetricExecutor implements OEventExecutor {

  public boolean canExecute(ODocument source, ODocument when) {

    String whenMetricName = when.field("type");
    String whenOperator = when.field("operator");
    String whenParameter = when.field("parameter");
    Number whenValue = (Number) OType.convert(when.field("value"), Number.class); // Integer

    String metricName = source.field("name");

    Object metricValue = source.field(whenParameter);
    Number metricValueD = (Number) OType.convert(metricValue, Number.class);

    if (metricName.equalsIgnoreCase(whenMetricName)) {

      if (whenOperator.equalsIgnoreCase(">=") && metricValueD.doubleValue() >= whenValue.doubleValue()) {
        return true;
      }
      if (whenOperator.equalsIgnoreCase("<=") && metricValueD.doubleValue() <= whenValue.doubleValue()) {
        return true;
      }
    }

    return false;

  }

  protected Map<String, Object> fillMapResolve(ODocument source, ODocument when) {

    return source.toMap();

  }

}
