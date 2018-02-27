package com.orientechnologies.orient.core.index;

public abstract class OIndexUpdateAction<V> {
  private static OIndexUpdateAction nothing = new OIndexUpdateAction() {
    @Override
    public boolean isNothing() {
      return true;
    }

    @Override
    public boolean isChange() {
      return false;
    }

    @Override
    public boolean isRemove() {
      return false;
    }

    @Override
    public Object getValue() {
      throw new UnsupportedOperationException();
    }
  };

  private static OIndexUpdateAction remove = new OIndexUpdateAction() {
    @Override
    public boolean isNothing() {
      return false;
    }

    @Override
    public boolean isChange() {
      return false;
    }

    @Override
    public boolean isRemove() {
      return true;
    }

    @Override
    public Object getValue() {
      throw new UnsupportedOperationException();
    }
  };

  public static OIndexUpdateAction nothing() {
    return nothing;
  }

  public static OIndexUpdateAction remove() {
    return remove;
  }

  public static <V> OIndexUpdateAction<V> changed(V newValue) {
    return new OIndexUpdateAction() {
      @Override
      public boolean isChange() {
        return true;
      }

      @Override
      public boolean isRemove() {
        return false;
      }

      @Override
      public boolean isNothing() {
        return false;
      }

      @Override
      public Object getValue() {
        return newValue;
      }
    };
  }

  public abstract boolean isChange();

  public abstract boolean isRemove();

  public abstract boolean isNothing();

  public abstract V getValue();
}
