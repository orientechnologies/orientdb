package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface UserRepository extends BaseRepository<OUser> {

  public OUser findUserByLogin(String login);

  public OUser findUserOrCreateByLogin(String login, Long id);

  public OUser findByGithubToken(String token);

  public List<Repository> findMyRepositories(String username);

  public List<Organization> findMyorganizationContributors(String username);

  public List<Organization> findMyorganization(String username);

  public List<Organization> findMyClientOrganization(String username);

  public Client findMyClientMember(String username, String organization);

  public List<Client> findAllMyClientMember(String username);

  public List<Environment> findMyEnvironment(OUser user);
}
