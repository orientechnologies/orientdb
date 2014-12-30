package com.orientechnologies.website.events;

import com.orientechnologies.website.services.reactor.ReactorMSG;
import reactor.event.Event;
import reactor.function.Consumer;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public abstract class EventInternal<T> implements Consumer<Event<T>> {

    public String handleWhat() {
        return ReactorMSG.INTERNAL_EVENT.toString() + "/" + event();
    }


    public abstract String event();

}
