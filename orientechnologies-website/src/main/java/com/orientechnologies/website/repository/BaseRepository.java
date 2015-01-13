package com.orientechnologies.website.repository;

import java.util.Collection;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public interface BaseRepository<T> {

    public T save(T entity);

    public void save(Collection<T> entities);

    public Class<T> getEntityClass();

    public T load(T entity);

    public void delete(T entity);
}
