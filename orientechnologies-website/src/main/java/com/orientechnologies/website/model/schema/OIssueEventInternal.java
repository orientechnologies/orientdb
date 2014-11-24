package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.IssueEventInternal;
import com.orientechnologies.website.model.schema.dto.OLabel;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.Date;

/**
 * Created by Enrico Risa on 07/11/14.
 */
public enum OIssueEventInternal implements OTypeHolder<IssueEventInternal> {
  VERSION("version") {
    @Override
    public OType getType() {
      return OType.EMBEDDED;
    }

  },
  PRIORITY("priority") {
    @Override
    public OType getType() {
      return OType.EMBEDDED;
    }

  };

  private String name;

  OIssueEventInternal(String name) {

    this.name = name;
  }

  @Override
  public IssueEventInternal fromDoc(ODocument doc, OrientBaseGraph graph) {
    IssueEventInternal event = new IssueEventInternal();
    event.setId(doc.getIdentity().toString());
    event.setEvent((String) doc.field(OIssueEvent.EVENT.toString()));
    event.setCreatedAt((Date) doc.field(OEvent.CREATED_AT.toString()));
    event.setActor(OUser.NAME.fromDoc((ODocument) doc.field(OIssueEvent.ACTOR.toString()), graph));
    event.setAssignee(OUser.NAME.fromDoc((ODocument) doc.field(OIssueEvent.ASSIGNEE.toString()), graph));
    event.setLabel(OLabel.NAME.fromDoc((ODocument) doc.field(OIssueEvent.LABEL.toString()), graph));
    event.setMilestone(OMilestone.TITLE.fromDoc((ODocument) doc.field(OIssueEvent.MILESTONE.toString()), graph));
    event.setVersion(OMilestone.TITLE.fromDoc((ODocument) doc.field(VERSION.toString()), graph));
    event.setPriority(OPriority.NAME.fromDoc((ODocument) doc.field(PRIORITY.toString()), graph));
    return event;
  }

  @Override
  public ODocument toDoc(IssueEventInternal entity, OrientBaseGraph graph) {

    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(OIssueEvent.UUID.toString(), java.util.UUID.randomUUID().toString());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(OIssueEvent.EVENT.toString(), entity.getEvent());
    doc.field(OIssueEvent.EVENT_ID.toString(), entity.getEventId());
    doc.field(OEvent.CREATED_AT.toString(), entity.getCreatedAt());
    doc.field(OIssueEvent.ACTOR.toString(), (entity.getActor() != null ? new ORecordId(entity.getActor().getRid()) : null));
    doc.field(OIssueEvent.ASSIGNEE.toString(), (entity.getAssignee() != null ? new ORecordId(entity.getAssignee().getRid()) : null));
    doc.field(OIssueEvent.LABEL.toString(), (entity.getLabel() != null ? OLabel.NAME.toDoc(entity.getLabel(), graph) : null));
    doc.field(OIssueEvent.MILESTONE.toString(),
        (entity.getMilestone() != null ? OMilestone.TITLE.toDoc(entity.getMilestone(), graph) : null));
    doc.field(VERSION.toString(), (entity.getVersion() != null ? OMilestone.TITLE.toDoc(entity.getVersion(), graph) : null));
    doc.field(PRIORITY.toString(), (entity.getPriority() != null ? OPriority.NAME.toDoc(entity.getPriority(), graph) : null));
    return doc;
  }

  @Override
  public String toString() {
    return name;
  }
}
