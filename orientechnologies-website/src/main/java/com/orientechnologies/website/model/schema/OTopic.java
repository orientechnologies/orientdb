package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.helper.OSequenceHelper;
import com.orientechnologies.website.model.schema.dto.Topic;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.Date;

/**
 * Created by Enrico Risa on 04/11/14.
 */

public enum OTopic implements OTypeHolder<Topic> {
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
  IID("iid") {
    @Override
    public OType getType() {
      return OType.LONG;
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
  CONFIDENTIAL("confidential") {
    @Override
    public OType getType() {
      return OType.BOOLEAN;
    }
  },
  STATE("state") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  ENVIRONMENT("environment") {
    @Override
    public OType getType() {
      return OType.EMBEDDED;
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
  UPDATED_AT("updatedAt") {
    @Override
    public OType getType() {
      return OType.DATETIME;
    }
  },
  DUE_TIME("dueTime") {
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

  OTopic(String name) {
    this.name = name;
  }

  @Override
  public ODocument toDoc(Topic entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(NUMBER.toString(), OSequenceHelper.next(graph, entity.getClass().getSimpleName()));
      doc.field(UUID.toString(), java.util.UUID.randomUUID().toString());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(BODY.toString(), entity.getBody());
    doc.field(CREATED_AT.toString(), entity.getCreatedAt());
    doc.field(UPDATED_AT.toString(), entity.getUpdatedAt());
    doc.field(TITLE.toString(), entity.getTitle());
    doc.field(CONFIDENTIAL.toString(), entity.getConfidential());
    return doc;
  }

  @Override
  public Topic fromDoc(ODocument doc, OrientBaseGraph graph) {
    Topic issue = new Topic();
    issue.setId(doc.getIdentity().toString());
    issue.setTitle((String) doc.field(TITLE.toString()));
    issue.setBody((String) doc.field(BODY.toString()));
    issue.setNumber((Integer) doc.field(NUMBER.toString()));
    issue.setCreatedAt((Date) doc.field(CREATED_AT.toString()));
    issue.setUpdatedAt((Date) doc.field(UPDATED_AT.toString()));
    issue.setUuid((String) doc.field(UUID.toString()));
    issue.setConfidential((Boolean) doc.field(CONFIDENTIAL.toString()));
    OrientVertex iss = graph.getVertex(doc);
    for (Vertex vertex : iss.getVertices(Direction.IN, HasTopic.class.getSimpleName())) {
      issue.setOrganization(OOrganization.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }

    for (Vertex vertex : iss.getVertices(Direction.OUT, HasOwner.class.getSimpleName())) {
      issue.setUser(OUser.NAME.fromDoc(((OrientVertex) vertex).getRecord(), graph));
      break;
    }
    long count = 0;
    for (Vertex vertex : iss.getVertices(Direction.OUT, HasTopicComment.class.getSimpleName())) {
      count += 1;
    }
    issue.setComments(count);

    return issue;
  }

  @Override
  public String toString() {
    return name;
  }
}
