package dijkstra.resources;

import jsr166y.ForkJoinPool;

public class Concurrency {

	static ForkJoinPool fork_join_pool = new ForkJoinPool();
	
	public static ForkJoinPool getForkJoinPool() {
		return fork_join_pool;
	}
}
