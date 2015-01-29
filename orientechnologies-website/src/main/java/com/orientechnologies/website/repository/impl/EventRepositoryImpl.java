package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.Event;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.IssueEvent;
import com.orientechnologies.website.repository.EventRepository;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 06/11/14.
 */
@Repository
public class EventRepositoryImpl extends OrientBaseRepository<Event> implements EventRepository {
  @Override
  public OTypeHolder<Event> getHolder() {
    return OEvent.CREATED_AT;
  }

  @Override
  public Class<Event> getEntityClass() {
    return Event.class;
  }

  @Override
  public Issue findIssueByEvent(IssueEvent event) {
    OrientGraph graph = dbFactory.getGraph();

    Vertex commVertex = graph.getVertex(new ORecordId(event.getId()));

    Iterable<Vertex> vertices = commVertex.getVertices(Direction.IN, HasEvent.class.getSimpleName());

    try {
      OrientVertex v = (OrientVertex) vertices.iterator().next();
      return OIssue.NUMBER.fromDoc(v.getRecord(), graph);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public IssueEvent reload(IssueEvent event) {
    OrientGraph graph = dbFactory.getGraph();
    return OIssueEvent.EVENT_ID.fromDoc(event.getInternal(), graph);
  }

}
