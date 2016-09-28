package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.github.GUser;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.model.schema.HasEnvironment;
import com.orientechnologies.website.model.schema.HasVersion;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import com.orientechnologies.website.repository.EnvironmentRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
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

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Service
public class UserServiceImpl implements UserService {

  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  private UserRepository         userRepository;

  @Autowired
  private OrganizationService    organizationService;

  @Autowired
  private OrganizationRepository organizationRepository;

  @Autowired
  private EnvironmentRepository  environmentRepository;

  @Autowired
  private RepositoryRepository   repositoryRepository;

  @Autowired
  protected GitHubConfiguration  gitHubConfiguration;

  @Transactional
  @Override
  public void initUser(String token) throws ServiceException {

    try {
      GitHub github = new GitHub(token, gitHubConfiguration);
      GUser self = github.user();
      String email = self.getEmail();

      OUser user = userRepository.findUserByLogin(self.getLogin());

      if (user == null) {

        throw ServiceException.create(401);

      } else {
        if (Boolean.TRUE.equals(user.getConfirmed()) || Boolean.TRUE.equals(user.getInvited())) {
          user.setId(self.getId());
          user.setToken(token);
          user.setEmail(email);
        } else {
          throw ServiceException.create(401);
        }

      }
      userRepository.save(user);

    } catch (Exception e) {
      if (e instanceof ServiceException) {
        ServiceException exception = (ServiceException) e;
        throw exception;
      }
      e.printStackTrace();

    }
  }

  @Override
  public UserDTO forWeb(OUser user) {
    UserDTO userDTO = new UserDTO();
    userDTO.setId(user.getId());
    userDTO.setName(user.getName());
    userDTO.setCompany(user.getCompany());
    userDTO.setWorkingEmail(user.getWorkingEmail());
    userDTO.setFirstName(user.getFirstName());
    userDTO.setSecondName(user.getSecondName());
    List<Repository> repositoryList = userRepository.findMyRepositories(user.getUsername());
    userDTO.setClientsOf(userRepository.findMyClientOrganization(user.getUsername()));
    userDTO.setClients(userRepository.findAllMyClientMember(user.getUsername()));
    userDTO.setContributorsOf(userRepository.findMyorganizationContributors(user.getUsername()));
    userDTO.setRepositories(repositoryList);
    userDTO.setConfirmed(user.getConfirmed());
    userDTO.setNotification(user.getNotification());
    userDTO.setWatching(user.getWatching());
    userDTO.setChatNotification(user.getChatNotification());
    userDTO.setPublicMute(user.getPublicMute());

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
  public boolean isSupport(OUser user, String orgName) {

    Client c = getClient(user, orgName);
    if (c != null) {
      return c.isSupport();
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
  public boolean isClient(OUser user, String orgName) {
    return userRepository.findMyClientMember(user.getUsername(), orgName) != null;
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

  @Transactional
  @Override
  public OUser patchUser(OUser current, UserDTO user) {
    if (user.getName().equalsIgnoreCase(current.getName())) {
      user.setRid(current.getRid());
      user.setConfirmed(true);
      return userRepository.save(user);
    }
    throw ServiceException.create(401);
  }

  @Override
  public void profileIssue(OUser current, Issue issue, String organization) {

    blankInfo(issue.getUser());

    if (issue.getAssignee() != null) {
      blankInfo(issue.getAssignee());
    }
    if (!isMember(current, organization) && !isSupport(current, organization) && !isCurrentClient(current, issue, organization)) {

      blankClientInfo(issue);
    } else {
      Client client = getClient(issue.getUser(), organization);

      if (client != null) {

        if (issue.getClient() != null) {
          List<Contract> clientContracts = organizationRepository.findClientContracts(organization, client.getClientId());

          List<Contract> collect = clientContracts.stream().filter(c -> {

            if (c.getFrom() != null && c.getTo() != null) {
              Date now = new Date();
              return c.getFrom().before(now) && c.getTo().after(now);
            }

            return false;
          }).collect(Collectors.toList());
          issue.getClient().setExpired(collect.size() == 0);
        }
        issue.getUser().setIsClient(true);
        issue.getUser().setClientName(client.getName());
        issue.getUser().setClientId(client.getClientId());
      }
    }
  }

  private boolean isCurrentClient(OUser current, Issue issue, String organization) {
    Client client1 = getClient(current, organization);
    return (issue.getClient() != null && client1 != null) ? issue.getClient().getClientId().equals(client1.getClientId()) : false;
  }

  private boolean isCurrentClient(String organization, String organization1) {

    return false;
  }

  private void blankClientInfo(Issue issue) {
    issue.setClient(null);
  }

  @Override
  public void profileEvent(OUser current, Event event, String organization) {
    if (!isMember(current, organization)) {
      if (event instanceof Comment) {
        blankInfo(((Comment) event).getUser());
      } else if (event instanceof IssueEvent) {
        blankInfo(((IssueEvent) event).getActor());
      }
    } else {
      if (event instanceof Comment) {

        Comment comment = (Comment) event;
        Client client = getClient(comment.getUser(), organization);
        if (client != null) {
          comment.getUser().setIsClient(true);
          comment.getUser().setClientName(client.getName());
          comment.getUser().setClientId(client.getClientId());
        }
      }

    }
  }

  @Override
  public void profileUser(OUser current, OUser toProfile, String organization) {

    if (!isMember(current, organization) && !isSupport(current, organization)) {
      blankInfo(toProfile);
    } else {
      Client client = getClient(toProfile, organization);
      if (client != null) {
        toProfile.setIsClient(true);
        toProfile.setClientName(client.getName());
        toProfile.setClientId(client.getClientId());
      }
    }
  }

  protected void blankInfo(OUser user) {
    user.setCompany("");
    user.setEmail("");
    user.setWorkingEmail("");
  }

  private void createUserEnvironmentRelationship(OUser client, Environment environment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(client.getRid()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(environment.getId()));
    orgVertex.addEdge(HasEnvironment.class.getSimpleName(), devVertex);
  }

}
