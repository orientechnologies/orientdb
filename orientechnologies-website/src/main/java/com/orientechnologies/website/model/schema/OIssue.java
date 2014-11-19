package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.OLabel;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 04/11/14.
 */

public enum OIssue implements OTypeHolder<Issue> {
  TITLE("title") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  UUID("uuid") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  BODY("body") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  NUMBER("number") {
    @Override
    public OType getType() {
      return OType.INTEGER;
    }
  },
  STATE("state") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  LABELS("labels") {
    @Override
    public OType getType() {
      return OType.EMBEDDEDSET;
    }
  },
  USER("user") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  },
  ASSIGNEE("assignee") {
    @Override
    public OType getType() {
      return OType.LINK;
    }
  },
  CREATED_AT("createdAt") {
    @Override
    public OType getType() {
      return OType.DATETIME;
    }
  },
  CLOSED_AT("closedAt") {
    @Override
    public OType getType() {
      return OType.DATETIME;
    }
  };
  private final String name;

  OIssue(String name) {
    this.name = name;
  }

  @Override
  public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Issue entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(UUID.toString(), java.util.UUID.randomUUID().toString());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(BODY.toString(), entity.getBody());
    doc.field(CREATED_AT.toString(), entity.getCreatedAt());
    doc.field(CLOSED_AT.toString(), entity.getClosedAt());
    doc.field(TITLE.toString(), entity.getTitle());
//    doc.field(LABELS.toString(), entity.getLabels());
    doc.field(NUMBER.toString(), entity.getNumber());
    doc.field(STATE.toString(), entity.getState());
    // MOVED TO EDGES
    // doc.field(ASSIGNEE.toString(), (entity.getAssignee() != null ? new ORecordId(entity.getAssignee().getId()) : null));
    // doc.field(USER.toString(), (entity.getUser() != null ? new ORecordId(entity.getUser().getId()) : null));
    return doc;
  }

  @Override
  public com.orientechnologies.website.model.schema.dto.Issue fromDoc(ODocument doc, OrientBaseGraph graph) {
    com.orientechnologies.website.model.schema.dto.Issue issue = new com.orientechnologies.website.model.schema.dto.Issue();
    issue.setId(doc.getIdentity().toString());
    issue.setTitle((String) doc.field(TITLE.toString()));
    issue.setBody((String) doc.field(BODY.toString()));
    issue.setState((String) doc.field(STATE.toString()));
    issue.setClosedAt((Date) doc.field(CLOSED_AT.toString()));
    issue.setCreatedAt((Date) doc.field(CREATED_AT.toString()));
    issue.setUuid((String) doc.field(UUID.toString()));
    issue.setNumber((Integer) doc.field(NUMBER.toString()));

    OrientVertex iss = graph.getVertex(doc);
    for (Vertex vertex : iss.getVertices(Direction.IN, HasIssue.class.getSimpleName())) {
      issue.setRepository(ORepository.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    for (Vertex vertex : iss.getVertices(Direction.OUT, HasMilestone.class.getSimpleName())) {
      issue.setMilestone(OMilestone.NUMBER.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    for (Vertex vertex : iss.getVertices(Direction.OUT, HasVersion.class.getSimpleName())) {
      issue.setVersion(OMilestone.NUMBER.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    List<Label> labelList = new ArrayList<Label>();
    for (Vertex vertex : iss.getVertices(Direction.OUT, HasLabel.class.getSimpleName())) {
      Label l = OLabel.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph);
      labelList.add(l);
    }
    for (Vertex vertex : iss.getVertices(Direction.OUT, IsAssigned.class.getSimpleName())) {
      issue.setAssignee(OUser.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    for (Vertex vertex : iss.getVertices(Direction.IN, HasOpened.class.getSimpleName())) {
      issue.setUser(OUser.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    long count = 0;
    for (Vertex vertex : iss.getVertices(Direction.OUT, HasEvent.class.getSimpleName())) {
      count += ((OrientVertex) vertex).getLabel().equals(Comment.class.getSimpleName()) ? 1 : 0;
    }
    issue.setComments(count);
    issue.setLabels(labelList);

    return issue;
  }

  @Override
  public String toString() {
    return name;
  }
}
