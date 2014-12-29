package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OScope;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Scope;
import com.orientechnologies.website.repository.ScopeRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 29/12/14.
 */
@Repository
public class ScopeRepositoryImpl extends OrientBaseRepository<Scope> implements ScopeRepository {


    @Override
    public OTypeHolder<Scope> getHolder() {
        return OScope.NAME;
    }

    @Override
    public Class<Scope> getEntityClass() {
        return Scope.class;
    }
}
