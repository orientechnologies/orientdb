package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.github.GUser;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.model.schema.HasEnvironment;
import com.orientechnologies.website.model.schema.HasVersion;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import com.orientechnologies.website.repository.EnvironmentRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.UserService;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Service
public class UserServiceImpl implements UserService {

  @Autowired
  private OrientDBFactory       dbFactory;

  @Autowired
  private UserRepository        userRepository;

  @Autowired
  private OrganizationService   organizationService;

  @Autowired
  private EnvironmentRepository environmentRepository;

  @Autowired
  private RepositoryRepository  repositoryRepository;

  @Transactional
  @Override
  public void initUser(String token) {

    try {
      GitHub github = new GitHub(token);
      GUser self = github.user();
      OUser user = userRepository.findUserByLogin(self.getLogin());
      String email = self.getEmail();

      if (user == null) {
        user = new OUser(self.getLogin(), token, email);
        user.setId(self.getId());
      } else {
        user.setToken(token);
        user.setEmail(email);
      }
      userRepository.save(user);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public UserDTO forWeb(OUser user) {
    UserDTO userDTO = new UserDTO();
    userDTO.setId(user.getId());
    userDTO.setName(user.getName());
    List<Repository> repositoryList = userRepository.findMyRepositories(user.getUsername());
    userDTO.setClientsOf(userRepository.findMyClientOrganization(user.getUsername()));
    userDTO.setClients(userRepository.findAllMyClientMember(user.getUsername()));
    userDTO.setRepositories(repositoryList);
    return userDTO;
  }

  @Override
  public boolean isMember(OUser user, String orgName) {

    List<Organization> myorganization = userRepository.findMyorganization(user.getUsername());
    for (Organization organization : myorganization) {
      if (organization.getName().equals(orgName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isTeamMember(OUser user, Repository repo) {
    return isMember(user, repo.getOrganization().getName());
  }

  @Override
  public Client getClient(OUser user, String orgName) {
    return userRepository.findMyClientMember(user.getUsername(), orgName);
  }

  @Override
  public Environment registerUserEnvironment(OUser user, Environment environment) {

    Integer version = environment.getVersionNumber();

    // To be handle in a better way the repo name for looking a milestone from id
    Milestone m = repositoryRepository.findMilestoneByRepoAndName(environment.getRepoName(), version);

    environment.setVersion(m);
    environment = environmentRepository.save(environment);
    // createVersionEnvironmentRelationship(environment, m);
    createUserEnvironmentRelationship(user, environment);
    return environmentRepository.load(environment);
  }

  @Override
  public void deregisterUserEnvironment(OUser user, Long environmentId) {

    Environment environment = environmentRepository.findById(environmentId);
    OrientGraph graph = dbFactory.getGraph();

    OrientVertex vertex = graph.getVertex(new ORecordId(environment.getId()));
    vertex.remove();

  }

  @Override
  public Environment patchUserEnvironment(OUser user, Long environmentId, Environment environment) {

    Environment env = environmentRepository.findById(environmentId);
    Integer version = environment.getVersionNumber();
    environment.setId(env.getId());
    // To be handle in a better way the repo name for looking a milestone from id
    Milestone m = repositoryRepository.findMilestoneByRepoAndName(environment.getRepoName(), version);
    environment.setVersion(m);
    environment = environmentRepository.save(environment);
    // createVersionEnvironmentRelationship(environment, m);
    return environmentRepository.load(environment);
  }

  private void createVersionEnvironmentRelationship(Environment environment, Milestone milestone) {

    if (milestone == null || (environment.getVersion() != null && environment.getVersion().getNumber() == milestone.getNumber())) {
      return;
    }
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(environment.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(milestone.getId()));

    for (Edge edge : orgVertex.getEdges(Direction.OUT, HasVersion.class.getSimpleName())) {
      edge.remove();
    }
    orgVertex.addEdge(HasVersion.class.getSimpleName(), devVertex);
  }

  @Override
  public List<Environment> getUserEnvironments(OUser user) {
    return userRepository.findMyEnvironment(user);
  }

  private void createUserEnvironmentRelationship(OUser client, Environment environment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(client.getRid()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(environment.getId()));
    orgVertex.addEdge(HasEnvironment.class.getSimpleName(), devVertex);
  }

}
