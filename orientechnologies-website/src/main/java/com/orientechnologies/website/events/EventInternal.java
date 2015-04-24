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

  protected String[] getActorsEmail(OUser owner, List<OUser> involvedActors, List<OUser> actorsInIssues) {
    Set<String> actors = new HashSet<String>();
    for (OUser involvedActor : involvedActors) {
      if (Boolean.TRUE.equals(involvedActor.getWatching())) {
        if (involvedActor.getEmail() != null && !involvedActor.getEmail().isEmpty()
            && !involvedActor.getEmail().equals(owner.getEmail())) {
          actors.add(involvedActor.getEmail());
        } else if (involvedActor.getWorkingEmail() != null && !involvedActor.getWorkingEmail().isEmpty()
            && !involvedActor.getWorkingEmail().equals(owner.getWorkingEmail())) {
          actors.add(involvedActor.getWorkingEmail());
        }
      }
    }
    for (OUser actorsInIssue : actorsInIssues) {
      if (!Boolean.TRUE.equals(actorsInIssue.getNotification())) {
        if (actorsInIssue.getEmail() != null && !actorsInIssue.getEmail().isEmpty()
            && !actorsInIssue.getEmail().equals(owner.getEmail())) {
          actors.remove(actorsInIssue.getEmail());
        } else if (actorsInIssue.getWorkingEmail() != null && !actorsInIssue.getWorkingEmail().isEmpty()
            && !actorsInIssue.getWorkingEmail().equals(owner.getWorkingEmail())) {
          actors.remove(actorsInIssue.getWorkingEmail());
        }
      }
    }
    return actors.toArray(new String[actors.size()]);
  }

}
