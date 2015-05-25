package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.model.schema.OContract;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.repository.ContractRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 12/05/15.
 */
@Repository
public class ContractRepositoryImpl extends OrientBaseRepository<Contract> implements ContractRepository {
  @Override
  public OTypeHolder<Contract> getHolder() {
    return OContract.NAME;
  }

  @Override
  public Class<Contract> getEntityClass() {
    return Contract.class;
  }

  @Override
  public Contract findByName(String orgName, String contractName) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select from (select expand(out('HasContract')[name = '%s']) from Organization where name = '%s') ", contractName, orgName);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<Contract> bots = new ArrayList<Contract>();

    try {
      OrientVertex vertex = vertices.iterator().next();
      ODocument doc = vertex.getRecord();
      Contract contract = OContract.NAME.fromDoc(doc, db);
      return contract;
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Contract findByUUID(String orgName, String uuid) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select from (select expand(out('HasContract')[uuid = '%s']) from Organization where name = '%s') ", uuid, orgName);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    try {
      OrientVertex vertex = vertices.iterator().next();
      ODocument doc = vertex.getRecord();
      Contract contract = OContract.NAME.fromDoc(doc, db);
      return contract;
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
