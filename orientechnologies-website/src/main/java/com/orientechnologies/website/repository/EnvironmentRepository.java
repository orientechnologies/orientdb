package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Environment;

/**
 * Created by Enrico Risa on 05/12/14.
 */
public interface EnvironmentRepository extends BaseRepository<Environment> {

  public Environment findById(Long environmentId);
}
