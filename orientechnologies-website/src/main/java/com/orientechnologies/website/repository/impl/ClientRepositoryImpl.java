package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OClient;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.repository.ClientRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 22/11/14.
 */
@Repository
public class ClientRepositoryImpl extends OrientBaseRepository<Client> implements ClientRepository {

  @Override
  public OTypeHolder<Client> getHolder() {
    return OClient.NAME;
  }

  @Override
  public Class<Client> getEntityClass() {
    return Client.class;
  }
}
