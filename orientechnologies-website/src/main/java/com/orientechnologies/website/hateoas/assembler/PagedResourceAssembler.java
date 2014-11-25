package com.orientechnologies.website.hateoas.assembler;

import com.orientechnologies.website.hateoas.Page;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 25/11/14.
 */

@Component
public class PagedResourceAssembler<T> implements ResourceAssembler<Page<T>, PagedResources<Resource<T>>> {

  @Override
  public PagedResources<Resource<T>> toResource(Page<T> tPage) {
    throw new UnsupportedOperationException();
  }

  public PagedResources<Resource<T>> toResource(Page<T> tPage, ResourceAssembler<T, Resource<T>> assembler) {
    return createResource(tPage, assembler);
  }

  private PagedResources<Resource<T>> createResource(Page<T> page, ResourceAssembler<T, Resource<T>> assembler) {
    List<Resource<T>> resources = new ArrayList<Resource<T>>();

    for (T t : page) {
      resources.add(assembler.toResource(t));
    }
    PagedResources<Resource<T>> pagedResources = new PagedResources<Resource<T>>(resources, asPageMetadata(page));

    return pagedResources;
  }

  private static <T> PagedResources.PageMetadata asPageMetadata(Page<T> page) {

    Assert.notNull(page, "Page must not be null!");
    return new PagedResources.PageMetadata(page.getSize(), page.getNumber(), page.getTotalElements(), page.getTotalPages());
  }

}
