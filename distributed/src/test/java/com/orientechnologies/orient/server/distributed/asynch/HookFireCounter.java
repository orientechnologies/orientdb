package com.orientechnologies.orient.server.distributed.asynch;

public class HookFireCounter {

	private static HookFireCounter instance;
	private static int vertexCreatedCnt;
	private static int vertexUpdatedCnt;
	private static int edgeCreatedCnt;
	private static int edgeUpdatedCnt;

	private HookFireCounter() { }

	public static HookFireCounter getInstance() {
		if (instance == null) {
			instance = new HookFireCounter();
		}
		return instance;
	}

	public static int getVertexCreatedCnt() {
		return vertexCreatedCnt;
	}

	public static void incrementVertexCreatedCnt() {
		vertexCreatedCnt++;
	}

	public static int getVertexUpdatedCnt() {
		return vertexUpdatedCnt;
	}

	public static void incrementVertexUpdatedCnt() {
		vertexUpdatedCnt++;
	}

	public static int getEdgeCreatedCnt() {
		return edgeCreatedCnt;
	}

	public static void incrementEdgeCreatedCnt() {
		edgeCreatedCnt++;
	}

	public static int getEdgeUpdatedCnt() {
		return edgeUpdatedCnt;
	}

	public static void incrementEdgeUpdatedCnt() {
		edgeUpdatedCnt++;
	}

}
