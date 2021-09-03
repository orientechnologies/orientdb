package com.orientechnologies.orient.object.enhancement;

public class SimpleSelfRef {

  private String name;
  private SimpleSelfRef friend;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SimpleSelfRef getFriend() {
    return friend;
  }

  public void setFriend(SimpleSelfRef friend) {
    this.friend = friend;
  }
}
