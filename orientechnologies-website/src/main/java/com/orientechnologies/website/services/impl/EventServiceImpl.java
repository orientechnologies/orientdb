package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.HasEvent;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.EventRepository;
import com.orientechnologies.website.services.EventService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * Created by Enrico Risa on 08/06/15.
 */
@Service
public class EventServiceImpl implements EventService {

  @Autowired
  private OrientDBFactory   dbFactory;

  @Autowired
  protected EventRepository eventRepository;

  @Override
  public void fireEvent(Issue issue, OUser actor, String event, OUser assignee, Label l) {

    IssueEventInternal e = new IssueEventInternal();
    e.setCreatedAt(new Date());
    e.setEvent(event);
    e.setAssignee(assignee);
    e.setActor(actor);
    e.setLabel(l);
    e = (IssueEventInternal) eventRepository.save(e);
    relateEvent(issue, e);
  }

  public void relateEvent(Issue issue, Event e) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(issue.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(e.getId()));

    orgVertex.addEdge(HasEvent.class.getSimpleName(), devVertex);
  }

}
