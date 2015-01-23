package com.orientechnologies.website.events;

import com.orientechnologies.website.services.reactor.ReactorMSG;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import reactor.core.Reactor;
import reactor.event.Event;

import java.util.Map;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Component
public class EventManager {

  @Autowired
  @Lazy
  protected Reactor reactor;

  public void pushEvent(String nameSpace, String event, Object payload) {
    throw new UnsupportedOperationException();
  }

  public void pushInternalEvent(String event, Object payload) {
    Map<String, Event<?>> eventMap = EventQueue.INSTANCE.get();
    eventMap.put(ReactorMSG.INTERNAL_EVENT + "/" + event, Event.wrap(payload));
  }

  public void fireEvents() {
    Map<String, Event<?>> eventMap = EventQueue.INSTANCE.get();
    for (String e : eventMap.keySet()) {
      reactor.notify(e, eventMap.get(e));
    }
    eventMap.clear();
  }
}
