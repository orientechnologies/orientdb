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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class OEventMetricExecutor implements OEventExecutor {
  protected Map<String, Object> body2name = new HashMap<String, Object>();

  public boolean canExecute(ODocument source, ODocument when) {

    String whenMetricName = when.field("name");
    String whenOperator = when.field("operator");
    String whenParameter = when.field("parameter");
    Double whenValue = when.field("value"); // Integer

    String metricName = source.field("name");

    Object metricValue = source.field(whenParameter);
    Double metricValueD = (Double) OType.convert(metricValue, Double.class);

    if (metricName.equalsIgnoreCase(whenMetricName)) {

      if (whenOperator.equalsIgnoreCase(">=") && metricValueD >= whenValue) {
        return true;
      }
      if (whenOperator.equalsIgnoreCase("<=") && metricValueD <= whenValue) {
        return true;
      }
    }

    return false;

  }

  protected void fillMapResolve(ODocument source, ODocument when) {

    this.getBody2name().clear();

    ODocument snapshot = source.field("snapshot");
    if (snapshot != null) {
      ODocument server = snapshot.field("server");
      if (server != null) {
        String serverName = server.field("name");
        this.getBody2name().put("servername", serverName);
      }
      Date dateFrom = snapshot.field("dateFrom");
      this.getBody2name().put("date", dateFrom);
    }
    String metricName = source.field("name");
    this.getBody2name().put("metric", metricName);

    String whenParameter = when.field("parameter");
    this.getBody2name().put("parameter", whenParameter);
    this.getBody2name().put("metricvalue", "" + source.field(whenParameter));

  }

  public Map<String, Object> getBody2name() {
    return body2name;
  }

  public void setBody2name(Map<String, Object> body2name) {
    this.body2name = body2name;
  }
}
