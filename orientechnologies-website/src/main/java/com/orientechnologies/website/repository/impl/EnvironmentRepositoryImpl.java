package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OEnvironment;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.repository.EnvironmentRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 05/12/14.
 */
@Repository
public class EnvironmentRepositoryImpl extends OrientBaseRepository<Environment> implements EnvironmentRepository {

  @Override
  public OTypeHolder<Environment> getHolder() {

    return OEnvironment.NAME;
  }

  @Override
  public Class<Environment> getEntityClass() {
    return Environment.class;
  }
}
