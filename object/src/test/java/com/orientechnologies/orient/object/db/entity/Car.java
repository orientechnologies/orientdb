package com.orientechnologies.orient.object.db.entity;

public class Car {

  private String name;
  private int year;

  public Car() {
    this.name = "Unknown";
    this.year = 0;
  }

  public Car(String name, int year) {
    this.name = name;
    this.year = year;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Car)) return false;

    Car car = (Car) o;

    if (year != car.year) return false;
    return name.equals(car.name);

  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + year;
    return result;
  }

  @Override
  public String toString() {
    return "Car{" +
             "name='" + name + '\'' +
             ", year=" + year +
             '}';
  }
}
