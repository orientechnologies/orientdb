package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.OUser;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface UserRepository extends BaseRepository<OUser> {

  public OUser findUserByLogin(String login);

  public OUser findUserOrCreateByLogin(String login, Long id);

  public OUser findByGithubToken(String token);
}
