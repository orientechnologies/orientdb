package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

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
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(BODY.toString(), entity.getBody());
    doc.field(CREATED_AT.toString(), entity.getCreatedAt());
    doc.field(CLOSED_AT.toString(), entity.getClosedAt());
    doc.field(TITLE.toString(), entity.getTitle());
    doc.field(LABELS.toString(), entity.getLabels());
    doc.field(NUMBER.toString(), entity.getNumber());
    doc.field(STATE.toString(), entity.getState());
    doc.field(ASSIGNEE.toString(), (entity.getAssignee() != null ? new ORecordId(entity.getAssignee().getId()) : null));
    doc.field(USER.toString(), (entity.getUser() != null ? new ORecordId(entity.getUser().getId()) : null));
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
    issue.setLabels(new ArrayList<String>((Collection<? extends String>) doc.field(LABELS.toString())));
    issue.setNumber((Integer) doc.field(NUMBER.toString()));

    OrientVertex iss = new OrientVertex(graph, doc);
    for (Vertex vertex : iss.getVertices(Direction.IN, HasIssue.class.getSimpleName())) {
      issue.setRepository(ORepository.CODENAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    issue.setAssignee(OUser.NAME.fromDoc((ODocument) doc.field(ASSIGNEE.toString()), graph));
    issue.setUser(OUser.NAME.fromDoc((ODocument) doc.field(USER.toString()), graph));
    return issue;
  }

  @Override
  public String toString() {
    return name;
  }
}
