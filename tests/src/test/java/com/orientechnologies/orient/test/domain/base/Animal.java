package com.orientechnologies.orient.test.domain.base;

public class Animal {
  private String name;

  public Animal(String name) {
    this.setName(name);
  }

  public Animal() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Animal)) return false;

    Animal animal = (Animal) o;

    if (!getName().equals(animal.getName())) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public String toString() {
    return "Animal{" + "name='" + getName() + '\'' + '}';
  }
}
