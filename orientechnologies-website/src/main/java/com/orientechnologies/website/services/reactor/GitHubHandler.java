package com.orientechnologies.website.services.reactor;

import reactor.function.Consumer;

import java.util.Set;

/**
 * Created by Enrico Risa on 27/10/14.
 */

public interface GitHubHandler<T> extends Consumer<T> {

  public Set<String> handleWhat();
}
