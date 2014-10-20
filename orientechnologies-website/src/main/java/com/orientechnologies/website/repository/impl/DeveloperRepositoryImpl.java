package com.orientechnologies.website.repository.impl;

import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Developer;
import com.orientechnologies.website.repository.DeveloperRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.OrientDBFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Created by Enrico Risa on 20/10/14.
 */

@Repository
public class DeveloperRepositoryImpl extends OrientBaseRepository<Developer> implements DeveloperRepository {

  @Override
  public Developer findUserByLogin(String login) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where username = '%s'", getEntityClass().getSimpleName(), login);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Class<Developer> getEntityClass() {
    return Developer.class;
  }

  @Override
  public ODocument toDoc(Developer entity) {
    ODocument doc = null;
    if (entity.getId() == null) {
      doc = new ODocument(OSiteSchema.Developer.class.getSimpleName());
    } else {
      doc = dbFactory.getGraph().getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(OSiteSchema.Developer.USERNAME.toString(), entity.getLogin());
    doc.field(OSiteSchema.Developer.TOKEN.toString(), entity.getToken());
    doc.field(OSiteSchema.Developer.EMAIL.toString(), entity.getEmail());
    return doc;
  }

  @Override
  public Developer fromDoc(ODocument doc) {
    Developer developer = new Developer();
    developer.setEmail((String) doc.field(OSiteSchema.Developer.EMAIL.toString()));
    developer.setId(doc.getIdentity().toString());
    developer.setLogin((String) doc.field(OSiteSchema.Developer.USERNAME.toString()));
    developer.setToken((String) doc.field(OSiteSchema.Developer.TOKEN.toString()));
    return developer;
  }
}
