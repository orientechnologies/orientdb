package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.UserRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 20/10/14.
 */

@Repository
public class UserRepositoryImpl extends OrientBaseRepository<OUser> implements UserRepository {

  @Override
  public OUser findUserByLogin(String login) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where name = '%s'", getEntityClass().getSimpleName(), login);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public OUser findUserOrCreateByLogin(String login, Long id) {

    OUser user = findUserByLogin(login);
    if (user == null) {
      user = new OUser(login, null, null);
      user.setId(id);
      user = save(user);
    }
    return user;
  }

  @Override
  public OUser findByGithubToken(String token) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where token = '%s'", getEntityClass().getSimpleName(), token);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Class<OUser> getEntityClass() {
    return OUser.class;
  }

  @Override
  public OTypeHolder<OUser> getHolder() {
    return com.orientechnologies.website.model.schema.OUser.EMAIL;
  }
}
