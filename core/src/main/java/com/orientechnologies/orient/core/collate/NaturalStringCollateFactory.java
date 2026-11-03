

package com.orientechnologies.orient.core.collate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NaturalStringCollateFactory implements OCollateFactory {


    private final Set<String> collateNames = new HashSet<>();

    public NaturalStringCollateFactory() {
        collateNames.add(NaturalStringCollate.NAME);
    }


    @Override
    public Set<String> getNames() {
        return Collections.unmodifiableSet(collateNames);
    }

    @Override
    public OCollate getCollate(String name) {
        if (NaturalStringCollate.NAME.equalsIgnoreCase(name)) {
            return new NaturalStringCollate();
        }
        return null;
    }
}