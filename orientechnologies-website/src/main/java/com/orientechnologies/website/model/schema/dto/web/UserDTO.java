package com.orientechnologies.website.model.schema.dto.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;

import java.util.List;

/**
 * Created by Enrico Risa on 26/11/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserDTO extends OUser {

  public List<Repository>   repositories;

  public List<Organization> clientsOf;

  public List<Organization> contributorsOf;

  public List<Repository> getRepositories() {
    return repositories;
  }

  private List<Client> clients;

  private Boolean      confirmed;



  @Override
  public Boolean getNotification() {
    return super.getNotification();
  }

  @Override
  public Boolean getWatching() {
    return super.getWatching();
  }

  @Override
  public Boolean getChatNotification() {
    return super.getChatNotification();
  }

  @Override
  public String getFirstName() {
    return super.getFirstName();
  }

  @Override
  public String getSecondName() {
    return super.getSecondName();
  }

  @Override
  public String getWorkingEmail() {
    return super.getWorkingEmail();
  }

  @Override
  public String getCompany() {
    return super.getCompany();
  }

  public void setRepositories(List<Repository> repositories) {
    this.repositories = repositories;
  }

  public void setClientsOf(List<Organization> clientsOf) {
    this.clientsOf = clientsOf;
  }

  public void setClients(List<Client> clients) {
    this.clients = clients;
  }

  public List<Client> getClients() {
    return clients;
  }

  public List<Organization> getClientsOf() {
    return clientsOf;
  }

  public Boolean getConfirmed() {
    return confirmed;
  }

  public void setConfirmed(Boolean confirmed) {
    this.confirmed = confirmed;
  }

  public void setContributorsOf(List<Organization> contributorsOf) {
    this.contributorsOf = contributorsOf;
  }

  public List<Organization> getContributorsOf() {
    return contributorsOf;
  }
}
