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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class OEventController extends Thread {

  private static OEventController                  instance;
  protected ODocument                              configuration;
  private Map<String, Map<String, OEventExecutor>> executors = new HashMap<String, Map<String, OEventExecutor>>();

  private ArrayBlockingQueue<OEvent>               events    = new ArrayBlockingQueue<OEvent>(10000);

  public OEventController(ODocument configuration) {
    this.configuration = configuration;

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
    Collection<Map<String, Object>> events = configuration.field("events");

    for (Map<String, Object> event : events) {

      Map<String, Object> when = (Map<String, Object>) event.get("when");

      String name = (String) when.get("name");

      if (eventType.toString().equalsIgnoreCase(name)) {
        matchingEvents.add(event);
      }
    }

    return matchingEvents;
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
