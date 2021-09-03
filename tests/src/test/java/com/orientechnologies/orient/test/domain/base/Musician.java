package com.orientechnologies.orient.test.domain.base;

import java.util.ArrayList;
import java.util.List;

public class Musician extends IdObject {
  private String name;
  private List<Instrument> instruments;

  public Musician() {
    super();
    this.setInstruments(new ArrayList<Instrument>());
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Instrument> getInstruments() {
    return instruments;
  }

  public void setInstruments(List<Instrument> instruments) {
    this.instruments = instruments;
  }

  @Override
  public String toString() {
    return "Musician [id="
        + this.getId()
        + ", version="
        + this.getVersion()
        + ", name="
        + this.getName()
        + ", instruments="
        + this.getInstruments()
        + "]";
  }
}
