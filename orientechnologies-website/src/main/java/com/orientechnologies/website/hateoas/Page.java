package com.orientechnologies.website.hateoas;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Enrico Risa on 25/11/14.
 */
public class Page<T> implements Iterable<T> {

  private long          number;
  private long          perPage;
  private long          totalElements;
  private Collection<T> elements;

  public Page(long number, long perPage, long totalElements, Collection<T> elements) {
    this.number = number;
    this.perPage = perPage;
    this.totalElements = totalElements;
    this.elements = elements;
  }

  @Override
  public Iterator<T> iterator() {

    return elements.iterator();
  }

  public long getSize() {
    return elements.size();
  }

  public long getNumber() {
    return number;
  }

  public long getTotalElements() {
    return totalElements;
  }

  public long getTotalPages() {
    return (long) Math.ceil(((getTotalElements() + .0) / perPage));
  }
}
