package com.orientechnologies.website.repository.impl;

import java.util.Collection;
import java.util.NoSuchElementException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

@Repository
public class OrganizationRepositoryImpl extends OrientBaseRepository<Organization> implements OrganizationRepository {

  @Autowired
  private OrientDBFactory dbFactory;

  @Override
  public Organization findOneByName(String name) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where codename = '%s'", getEntityClass().getSimpleName(), name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Organization save(Organization entity) {

    OrientGraph db = dbFactory.getGraph();
    db.getRawGraph().save(toDoc(entity));
    return entity;
  }

  @Override
  public void save(Collection<Organization> entities) {

  }

  @Override
  public Class<Organization> getEntityClass() {
    return Organization.class;
  }

  @Override
  public ODocument toDoc(Organization entity) {

    ODocument doc = new ODocument(entity.getClass().getSimpleName());
    doc.field(OSiteSchema.Organization.NAME.toString(), entity.getName());
    doc.field(OSiteSchema.Organization.CODENAME.toString(), entity.getCodename());
    return doc;
  }

  @Override
  public Organization fromDoc(ODocument doc) {

    Organization organization = new Organization();

    organization.setId(doc.getIdentity().toString());
    organization.setName((String) doc.field(OSiteSchema.Organization.NAME.toString()));
    organization.setCodename((String) doc.field(OSiteSchema.Organization.CODENAME.toString()));

    return organization;
  }
}
