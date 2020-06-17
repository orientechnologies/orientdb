package com.orientechnologies.orient.test.domain.base;

import java.util.Set;

public class SimplePerson {
  private String name;
  private Set<String> animals;

  public SimplePerson(String name, Set<String> animals) {
    this.setName(name);
    this.setAnimals(animals);
  }

  public SimplePerson() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<String> getAnimals() {
    return animals;
  }

  public void setAnimals(Set<String> animals) {
    this.animals = animals;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SimplePerson)) return false;

    SimplePerson person = (SimplePerson) o;

    if (!getAnimals().equals(person.getAnimals())) return false;
    if (!getName().equals(person.getName())) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getAnimals().hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SimplePerson{" + "name='" + getName() + '\'' + ", animals=" + getAnimals() + '}';
  }
}
