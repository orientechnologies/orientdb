package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Index;

public interface OrientIndex<T extends OrientElement> extends Index<T> {
  void removeElement(final T element);
}
