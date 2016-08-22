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
package com.orientechnologies.agent.event;

import com.orientechnologies.agent.plugins.OEventPlugin;
import com.orientechnologies.agent.profiler.OProfilerData;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerEntry;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class OEventController extends Thread {

  private Map<String, Map<String, OEventExecutor>> executors = new HashMap<String, Map<String, OEventExecutor>>();

  private ArrayBlockingQueue<OEvent>               events    = new ArrayBlockingQueue<OEvent>(10000);
  private OEventPlugin                             owner;

  public OEventController(OEventPlugin owner) {
    this.owner = owner;

    configureExecutors();
  }

  private void configureExecutors() {
    register(new OEventMetricMailExecutor());
    register(new OEventLogMailExecutor());
    register(new OEventLogFunctionExecutor());
    register(new OEventMetricFunctionExecutor());
    register(new OEventLogHttpExecutor());
    register(new OEventMetricHttpExecutor());
  }

  public OEventExecutor getExecutor(String when, String what) {
    return executors.get(when).get(what);
  }

  public void register(OEventExecutor e) {
    EventConfig a = e.getClass().getAnnotation(EventConfig.class);
    String when = a.when();
    String what = a.what();
    if (executors.get(when) == null) {
      executors.put(when, new HashMap<String, OEventExecutor>());
    }
    executors.get(when).put(what, e);

  }

  public void broadcast(OEvent.EVENT_TYPE type, ODocument payload) {
    events.offer(new OEvent(type, payload));
  }

  protected Collection<Map<String, Object>> getEvents(OEvent.EVENT_TYPE eventType) {
    List<Map<String, Object>> matchingEvents = new ArrayList<Map<String, Object>>();
    Collection<Map<String, Object>> events = owner.getConfig().field("events");

    for (Map<String, Object> event : events) {

      Map<String, Object> when = (Map<String, Object>) event.get("when");

      String name = (String) when.get("name");

      if (eventType.toString().equalsIgnoreCase(name)) {
        matchingEvents.add(event);
      }
    }

    return matchingEvents;
  }

  public void analyzeSnapshot(OProfilerData profilerData) {

    List<Map<String, Object>> matchingEvents = new ArrayList<Map<String, Object>>();
    Collection<Map<String, Object>> events = owner.getConfig().field("events");

    for (Map<String, Object> event : events) {
      Map<String, Object> when = (Map<String, Object>) event.get("when");
      String name = (String) when.get("name");
      if (OEvent.EVENT_TYPE.METRIC_WHEN.toString().equalsIgnoreCase(name)) {
        matchingEvents.add(event);
      }
    }

    for (Map<String, Object> matchingEvent : matchingEvents) {
      Map<String, Object> when = (Map<String, Object>) matchingEvent.get("when");
      String type = (String) when.get("type");

      ODocument oDocument = metricToODocument(type, profilerData);

      if (oDocument != null) {
        broadcast(OEvent.EVENT_TYPE.METRIC_WHEN, oDocument);
      }
    }

  }

  private ODocument metricToODocument(String type, OProfilerData profilerData) {
    OPair<String, OProfiler.METRIC_TYPE> mType = Orient.instance().getProfiler().getMetadata().get(type);

    ODocument metric = null;
    if (mType != null) {
      switch (mType.value) {
      case COUNTER:

        long counter = profilerData.getCounter(type);

        metric = new ODocument();
        metric.field("name", type);
        metric.field("value", counter);

      case CHRONO:

        OProfilerEntry chrono;
        if (type.startsWith("db.*")) {
          chrono = sumDBValues(type, profilerData);
        } else {
          chrono = profilerData.getChrono(type);
        }

        if (chrono != null) {
          metric = new ODocument();
          metric.field("name", chrono.name);
          metric.field("entries", chrono.entries);
          metric.field("average", chrono.average);
          metric.field("max", chrono.max);
          metric.field("min", chrono.min);
          metric.field("last", chrono.last);
          metric.field("total", chrono.total);
          return metric;
        }
        break;
      case STAT:

        OProfilerEntry stat = profilerData.getStat(type);

        if (stat != null) {
          metric = new ODocument();
          metric.field("name", stat.name);
          metric.field("entries", stat.entries);
          metric.field("average", stat.average);
          metric.field("max", stat.max);
          metric.field("min", stat.min);
          metric.field("last", stat.last);
          metric.field("total", stat.total);

        }
      }
      metric.field("from", new Date(profilerData.getRecordingFrom()));
      metric.field("to", new Date(profilerData.getRecordingTo()));
      metric.field("server", owner.getNodeName());
      return metric;
    }
    return null;
  }

  private OProfilerEntry sumDBValues(String type, OProfilerData profilerData) {
    String hookValue = (String) profilerData.getHookValue("system.databases");
    OProfilerEntry entry = null;
    if (hookValue != null) {
      String[] split = hookValue.split(",");
      for (String s : split) {
        OProfilerEntry chrono = profilerData.getChrono(type.replaceFirst("\\*", s));
        if (chrono != null) {
          if (entry == null) {
            entry = new OProfilerEntry();
            entry.average = chrono.average;
            entry.max = chrono.max;
            entry.min = chrono.min;
            entry.total = chrono.total;
            entry.entries = chrono.entries;
            entry.last = chrono.last;
            entry.name = type;
          } else {

            entry.average += chrono.average;
            entry.max += chrono.max;
            entry.min += chrono.min;
            entry.total += chrono.total;
            entry.entries += chrono.entries;
            entry.last += chrono.last;
          }
        }
      }
    }
    return entry;
  }

  @Override
  public void run() {

    OEvent evt = null;
    while (true) {
      try {
        evt = events.take();

        Collection<Map<String, Object>> events = getEvents(evt.getType());

        for (Map<String, Object> event : events) {
          Map<String, Object> when = (Map<String, Object>) event.get("when");
          Map<String, Object> what = (Map<String, Object>) event.get("what");
          OEventExecutor executor = executors.get(when.get("name")).get(what.get("name"));
          executor.execute(evt.getPayload(), new ODocument().fromMap(when), new ODocument().fromMap(what));
        }
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {

        if (evt != null) {

          OLogManager.instance().warn(this, "Cannot broadcast event EVENT when=%s -> %s", evt.getType(), evt.getPayload());
        }
      }
    }
  }

}
