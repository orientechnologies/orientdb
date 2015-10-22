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

  public List<Repository> getRepositories() {
    return repositories;
  }

  private List<Client> clients;

  private Boolean      confirmed;


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
}
