package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.User;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface UserRepository extends BaseRepository<User> {

  public User findUserByLogin(String login);

  public User findByGithubToken(String token);
}
