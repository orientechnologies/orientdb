package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.core.annotation.OBeforeSerialization;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class AbstractEntity {
  private boolean before1Called = false;
  private boolean before2Called = false;

  public void reset() {
    before1Called = false;
    before2Called = false;
  }

  @OBeforeSerialization
  public void before1() {
    before1Called = true;
  }

  @OBeforeSerialization
  public void before2() {
    before2Called = true;
  }

  public boolean callbackExecuted() {
    return before1Called && before2Called;
  }
}
