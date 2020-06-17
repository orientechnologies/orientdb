package com.orientechnologies.orient.test.domain.base;

import java.util.Set;
import javax.persistence.OneToMany;

public class ComplicatedPerson {
  private String name;

  @OneToMany(orphanRemoval = true)
  private Set<Animal> animals;

  public ComplicatedPerson(String name, Set<Animal> animals) {
    this.setName(name);
    this.setAnimals(animals);
  }

  public ComplicatedPerson() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<Animal> getAnimals() {
    return animals;
  }

  public void setAnimals(Set<Animal> animals) {
    this.animals = animals;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ComplicatedPerson)) return false;

    ComplicatedPerson that = (ComplicatedPerson) o;

    if (!getAnimals().equals(that.getAnimals())) return false;
    if (!getName().equals(that.getName())) return false;

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
    return "ComplicatedPerson{" + "name='" + getName() + '\'' + ", animals=" + getAnimals() + '}';
  }
}
