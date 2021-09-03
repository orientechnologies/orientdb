package com.orientechnologies.orient.etl.http;

/** Created by gabriele on 27/02/17. */
public interface OETLListener {

  void onEnd(OETLJob oetlJob);
}
