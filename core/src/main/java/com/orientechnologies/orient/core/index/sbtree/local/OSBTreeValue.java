package com.orientechnologies.orient.core.index.sbtree.local;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 9/27/13
 */
public class OSBTreeValue<V> {
  private final boolean isLink;
  private final long    link;
  private final V       value;

  public OSBTreeValue(boolean isLink, long link, V value) {
    this.isLink = isLink;
    this.link = link;
    this.value = value;
  }

  public boolean isLink() {
    return isLink;
  }

  public long getLink() {
    return link;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OSBTreeValue that = (OSBTreeValue) o;

    if (isLink != that.isLink)
      return false;
    if (link != that.link)
      return false;
    if (value != null ? !value.equals(that.value) : that.value != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (isLink ? 1 : 0);
    result = 31 * result + (int) (link ^ (link >>> 32));
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OSBTreeValue{" + "isLink=" + isLink + ", link=" + link + ", value=" + value + '}';
  }
}
