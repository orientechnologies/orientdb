package com.orientechnologies.website.events;

import reactor.event.Event;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public class EventQueue extends ThreadLocal<Map<String, Event<?>>> {


    public static volatile EventQueue INSTANCE = new EventQueue();


    @Override
    public Map<String, Event<?>> get() {
        Map<String, Event<?>> eventMap = super.get();
        if (eventMap == null) {
            eventMap = new LinkedHashMap<String, Event<?>>();
            set(eventMap);
        }
        return eventMap;
    }

    public void clear() {
        set(new LinkedHashMap<String, Event<?>>());
    }
}
