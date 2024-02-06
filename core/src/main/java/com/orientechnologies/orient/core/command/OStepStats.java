package com.orientechnologies.orient.core.command;

public class OStepStats {
  long begin = -1;
  long totalCost = -1;
  long count = 0;

  public void start() {
    this.begin = System.nanoTime();
    this.count += 1;
  }

  public void end() {
    if (begin != -1) {
      this.totalCost += (System.nanoTime() - begin);
    }
    begin = -1;
  }

  public void pause() {
    if (begin != -1) {
      this.totalCost += (System.nanoTime() - begin);
    }
    begin = -1;
  }

  public void resume() {
    this.begin = System.nanoTime();
  }

  public long getCost() {
    return totalCost;
  }

  public long getCount() {
    return count;
  }
}
