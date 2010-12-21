package com.orientechnologies.orient.core.sql.functions;

public abstract class OSQLFunctionAbstract implements OSQLFunction {
	protected String	name;
	private int				minParams;
	private int				maxParams;

	public OSQLFunctionAbstract(final String iName, final int iMinParams, final int iMaxParams) {
		this.name = iName;
		this.minParams = iMinParams;
		this.maxParams = iMaxParams;
	}

	public String getName() {
		return name;
	}

	public int getMinParams() {
		return minParams;
	}

	public int getMaxParams() {
		return maxParams;
	}

	@Override
	public String toString() {
		return name + "()";
	}
}
