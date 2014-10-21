package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Repository;

public interface RepositoryService {

  public Repository createRepo(String name, String description);
}
