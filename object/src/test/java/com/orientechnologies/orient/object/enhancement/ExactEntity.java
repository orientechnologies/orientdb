package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.core.annotation.OBeforeSerialization;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class ExactEntity extends AbstractEntity {
  private boolean before3Called = false;

  public void reset() {
    super.reset();
    before3Called = false;
  }

  @OBeforeSerialization
  public void before3() {
    before3Called = true;
  }

  @Override
  public boolean callbackExecuted() {
    return super.callbackExecuted() && before3Called;
  }
}
