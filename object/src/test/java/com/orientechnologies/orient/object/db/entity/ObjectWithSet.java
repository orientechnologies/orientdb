package com.orientechnologies.orient.object.db.entity;

import javax.persistence.Id;
import javax.persistence.OneToMany;
import java.util.HashSet;
import java.util.Set;

public class ObjectWithSet {

  @Id
  public String id;

  @OneToMany
  Set<ObjectWithSet> friends = new HashSet<>();

  String name;

  public Set<ObjectWithSet> getFriends() {
    return this.friends;
  }

  public void addFriend(ObjectWithSet friend) {
    this.friends.add(friend);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
