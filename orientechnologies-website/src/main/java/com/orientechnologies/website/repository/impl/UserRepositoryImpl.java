package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.UserRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
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
  public List<com.orientechnologies.website.model.schema.dto.Repository> findMyRepositories(String username) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(in('HasMember')[@class = 'Repository']) from %s where name = '%s'",
        getEntityClass().getSimpleName(), username);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<com.orientechnologies.website.model.schema.dto.Repository> repositories = new ArrayList<com.orientechnologies.website.model.schema.dto.Repository>();
    for (OrientVertex vertice : vertices) {
      repositories.add(ORepository.NAME.fromDoc(vertice.getRecord(), db));
    }
    return repositories;
  }

  @Override
  public List<Organization> findMyorganization(String username) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(in('HasMember')[@class = 'Organization']) from %s where name = '%s'",
        getEntityClass().getSimpleName(), username);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<Organization> organization = new ArrayList<Organization>();
    for (OrientVertex vertice : vertices) {
      organization.add(OOrganization.NAME.fromDoc(vertice.getRecord(), db));
    }
    return organization;
  }

  @Override
  public List<Organization> findMyorganizationContributors(String username) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(in('HasContributor')[@class = 'Organization']) from %s where name = '%s'",
        getEntityClass().getSimpleName(), username);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<Organization> organization = new ArrayList<Organization>();
    for (OrientVertex vertice : vertices) {
      organization.add(OOrganization.NAME.fromDoc(vertice.getRecord(), db));
    }
    return organization;
  }

  @Override
  public List<Organization> findMyClientOrganization(String username) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(in('HasMember')[@class = 'Client'].in('HasClient')) from %s where name = '%s'",
        getEntityClass().getSimpleName(), username);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<Organization> organization = new ArrayList<Organization>();
    for (OrientVertex vertice : vertices) {
      organization.add(OOrganization.NAME.fromDoc(vertice.getRecord(), db));
    }
    return organization;
  }

  @Override
  public Client findMyClientMember(String username, String organization) {
    OrientGraph db = dbFactory.getGraph();
    String query = String
        .format(
            "select from (select expand(in('HasMember')[@class = 'Client']) from %s where name = '%s') where in('HasClient').name CONTAINS '%s'",
            getEntityClass().getSimpleName(), username, organization);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return OClient.CLIENT_ID.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<Client> findAllMyClientMember(String username) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from (select expand(in('HasMember')[@class = 'Client']) from %s where name = '%s')",
        getEntityClass().getSimpleName(), username);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    List<Client> clients = new ArrayList<Client>();
    for (OrientVertex v : vertices) {
      clients.add(OClient.CLIENT_ID.fromDoc(v.getRecord(), db));
    }
    return clients;
  }

  @Override
  public List<Environment> findMyEnvironment(OUser user) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from (select expand(out('HasEnvironment')) from %s where name = '%s')", getEntityClass()
        .getSimpleName(), user.getName());
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    List<Environment> clients = new ArrayList<Environment>();
    for (OrientVertex v : vertices) {
      clients.add(OEnvironment.EID.fromDoc(v.getRecord(), db));
    }
    return clients;
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
