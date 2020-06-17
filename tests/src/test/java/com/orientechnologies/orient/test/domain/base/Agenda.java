package com.orientechnologies.orient.test.domain.base;

import java.util.LinkedList;
import java.util.List;

public class Agenda {
  public Agenda() {}

  @javax.persistence.Id private String id;
  @javax.persistence.Version private Long version;

  public String getId() {
    return id;
  }

  @javax.persistence.Embedded private List<Event> events = new LinkedList<Event>();

  public List<Event> getEvents() {
    return events;
  }

  public void setEvents(List<Event> events) {
    this.events = events;
  }
}
