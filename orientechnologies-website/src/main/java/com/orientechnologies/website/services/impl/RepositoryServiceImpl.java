package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.RepositoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@Service
public class RepositoryServiceImpl implements RepositoryService {

  @Autowired
  protected RepositoryRepository repositoryRepository;

  @Override
  public Repository createRepo(String name, String description) {
    Repository repo = new Repository();
    repo.setCodename(name);
    repo.setName(description);
    return repositoryRepository.save(repo);
  }
}
