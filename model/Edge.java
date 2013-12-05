/*
 * Dijkstra Java code from
 * 
 * http://www.vogella.com/articles/JavaAlgorithmsDijkstra/article.html
 * 
 * Version 1.1 - Copyright 2009, 2010, 2011, 2011 Lars Vogel
 * 
 * Eclipse Public License
 */

package dijkstra.model;

public class Edge {
	private final String id;
	private final Vertex source;
	private final Vertex destination;
	private final int weight;

	public Edge(String id, Vertex source, Vertex destination, int weight) {
		this.id = id;
		this.source = source;
		this.destination = destination;
		this.weight = weight;
	}

	public String getId() {
		return id;
	}

	public Vertex getDestination() {
		return destination;
	}

	public Vertex getSource() {
		return source;
	}

	public int getWeight() {
		return weight;
	}

	@Override
	public String toString() {
		return source + " " + destination + " " + weight;
	}
}
