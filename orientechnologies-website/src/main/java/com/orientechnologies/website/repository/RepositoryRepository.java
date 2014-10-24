package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Repository;

/**
 * Created by Enrico Risa on 21/10/14.
 */
public interface RepositoryRepository extends BaseRepository<Repository> {

  public Repository findByOrgAndName(String org, String name);
}
