package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Contract;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 12/05/15.
 */
public enum OContract implements OTypeHolder<Contract> {
  NAME("name") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  BUSINESS_HOURS("businessHours") {
    @Override
    public OType getType() {
      return OType.EMBEDDEDLIST;
    }
  },
  UUID("uuid") {
    @Override
    public OType getType() {
      return OType.STRING;
    }
  },
  SLAS("slas") {
    @Override
    public OType getType() {
      return OType.EMBEDDEDMAP;
    }
  };
  private final String name;

  @Override
  public String toString() {
    return name;
  }

  OContract(String name) {
    this.name = name;
  }

  @Override
  public Contract fromDoc(ODocument doc, OrientBaseGraph graph) {
    Contract contract = new Contract();
    contract.setId(doc.getIdentity().toString());
    contract.setName((String) doc.field(NAME.toString()));
    contract.setUuid((String) doc.field(UUID.toString()));
    contract.setBusinessHours((List<String>) doc.field(BUSINESS_HOURS.toString()));
    contract.setSlas((Map<Integer, Integer>) doc.field(SLAS.toString()));
    return contract;
  }

  @Override
  public ODocument toDoc(Contract entity, OrientBaseGraph graph) {
    ODocument doc;
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      doc.field(NAME.toString(), entity.getName());
      doc.field(UUID.toString(), java.util.UUID.randomUUID().toString());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(NAME.toString(), entity.getName());
    doc.field(BUSINESS_HOURS.toString(), entity.getBusinessHours());
    doc.field(SLAS.toString(), entity.getSlas());
    return doc;
  }
}
