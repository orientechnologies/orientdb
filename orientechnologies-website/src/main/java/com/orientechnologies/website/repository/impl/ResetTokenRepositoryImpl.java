package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.ResetToken;
import com.orientechnologies.website.repository.ResetTokenRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 05/07/2018.
 */
@org.springframework.stereotype.Repository
public class ResetTokenRepositoryImpl extends OrientBaseRepository<ResetToken> implements ResetTokenRepository {

  @Override
  public ResetToken findByToken(String token) {
    OrientGraph graph = dbFactory.getGraph();

    String query = String.format("select from %s where %s = '%s' ", getEntityClass().getSimpleName(), OResetToken.TOKEN, token);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public OTypeHolder<ResetToken> getHolder() {
    return OResetToken.TOKEN;
  }

  @Override
  public Class<ResetToken> getEntityClass() {
    return ResetToken.class;
  }
}
