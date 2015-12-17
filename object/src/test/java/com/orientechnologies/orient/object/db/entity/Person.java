package com.orientechnologies.orient.object.db.entity;


import java.util.HashMap;
import java.util.Map;

import javax.persistence.Embedded;

public class Person {

  private String           name;
  @Embedded
  private Map<String, Car> placeToCar;

  public Person() {
    this.name = "Unknown";
    this.placeToCar = new HashMap<String,Car>();
  }

  public Person(String name, Map<String, Car> placeToCar) {
    this.name = name;
    this.placeToCar = placeToCar;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, Car> getPlaceToCar() {
    return placeToCar;
  }

  public void setPlaceToCar(Map<String, Car> placeToCar) {
    this.placeToCar = placeToCar;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Person))
      return false;

    Person person = (Person) o;

    if (!name.equals(person.name))
      return false;
    return placeToCar.equals(person.placeToCar);

  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + placeToCar.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Person{" +
             "name='" + name + '\'' +
             ", placeToCar=" + placeToCar +
             '}';
  }
}
