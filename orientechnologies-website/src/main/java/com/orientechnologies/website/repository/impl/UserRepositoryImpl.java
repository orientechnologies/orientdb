package com.orientechnologies.website.repository.impl;

import java.util.NoSuchElementException;

import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.User;
import com.orientechnologies.website.repository.UserRepository;
import org.springframework.stereotype.Repository;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Created by Enrico Risa on 20/10/14.
 */

@Repository
public class UserRepositoryImpl extends OrientBaseRepository<User> implements UserRepository {

  @Override
  public User findUserByLogin(String login) {

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
  public User findUserOrCreateByLogin(String login) {

    User user = findUserByLogin(login);
    if (user == null) {
      user = new User(login, null, null);
      user = save(user);
    }
    return user;
  }

  @Override
  public User findByGithubToken(String token) {
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
  public Class<User> getEntityClass() {
    return User.class;
  }

  @Override
  public OSiteSchema.OTypeHolder<User> getHolder() {
    return OSiteSchema.User.EMAIL;
  }
}
