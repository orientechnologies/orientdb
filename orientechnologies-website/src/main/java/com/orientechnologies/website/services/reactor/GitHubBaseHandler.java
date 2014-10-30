package com.orientechnologies.website.services.reactor;

import reactor.function.Consumer;

import java.util.List;

/**
 * Created by Enrico Risa on 27/10/14.
 */
public interface GitHubBaseHandler<T> extends Consumer<T> {

  public List<String> handleWhat();

}
