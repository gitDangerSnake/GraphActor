package edu.hnu.cg.graph.userdefine;

public interface GraphConfig {

	public static final String vertexToEdgesSep = "->";
	public static final int sectionSize = 1000000;
	public static final int numVertices = 1000000;
	public static final int cachelineSize = 64; // default settings
	public static final int numWorkers = 1000000;
	public static final int verticesToWoker = 1;
	public static final int totalVertices = 1000000;
}
