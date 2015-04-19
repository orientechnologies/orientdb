package com.tinkerpop.blueprints.util.io.graphson;

import java.util.List;

import com.tinkerpop.blueprints.Element;

public interface IGraphSONIterable {
	public List<? extends Element> getList();
}
