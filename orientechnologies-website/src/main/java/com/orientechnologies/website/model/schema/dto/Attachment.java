package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 06/11/15.
 */
public class Attachment {

  public String name;
  public long   size;
  public String type;
  public long   mTime;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getmTime() {
    return mTime;
  }

  public void setmTime(long mTime) {
    this.mTime = mTime;
  }
}
