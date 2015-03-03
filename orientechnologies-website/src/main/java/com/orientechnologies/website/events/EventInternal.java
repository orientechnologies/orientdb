package com.orientechnologies.website.events;

import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.services.reactor.ReactorMSG;
import reactor.event.Event;
import reactor.function.Consumer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public abstract class EventInternal<T> implements Consumer<Event<T>> {

  public String handleWhat() {
    return ReactorMSG.INTERNAL_EVENT.toString() + "/" + event();
  }

  public abstract String event();

  protected String[] getActorsEmail(OUser owner, List<OUser> involvedActors) {
    Set<String> actors = new HashSet<String>();
    for (OUser involvedActor : involvedActors) {
      if (Boolean.TRUE.equals(involvedActor.getNotification())) {
        if (involvedActor.getEmail() != null && !involvedActor.getEmail().equals(owner.getEmail())) {
          actors.add(involvedActor.getEmail());
        } else if (involvedActor.getWorkingEmail() != null && !involvedActor.getWorkingEmail().equals(owner.getWorkingEmail())) {
          actors.add(involvedActor.getWorkingEmail());
        }
      }
    }
    return actors.toArray(new String[actors.size()]);
  }
}
