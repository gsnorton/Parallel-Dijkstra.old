/*
 * BASED ON Dijkstra Java code from
 * 
 * http://www.vogella.com/articles/JavaAlgorithmsDijkstra/article.html
 * 
 * Version 1.1 - Copyright 2009, 2010, 2011, 2011 Lars Vogel
 * 
 * MODIFIED BY Gregory Norton to address performance concerns and eliminate
 * requirement to store nodes external to the object. Further modifications
 * added the parallelization.
 * 
 * Eclipse Public License
 * 
 * HT for Fork/Join HowTo: http://www.javacodegeeks.com/2011/02/
 * 							 java-forkjoin-parallel-programming.html
 */

package dijkstra.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;
import dijkstra.model.Edge;
import dijkstra.model.Graph;
import dijkstra.model.Vertex;

public class DijkstraAlgorithm {
	
	private static final int MAX_PROCESSING_SPLIT_COUNT = 1;
	
	private Set<Vertex> settled_nodes;
	private Map<Vertex, Vertex> predecessors;
	
	private class UnsettledNode implements Comparable<UnsettledNode> {
		
		int distance; Vertex node;
		
		public UnsettledNode(final int distance, final Vertex node) {
			this.distance = distance; this.node = node;
		}

		@Override
		public int compareTo(final UnsettledNode other) {
			return (distance - other.distance);
		}
	}
	
	/* -------------------------------------------------------------------- */
	
	private List<UnsettledNode> global_unsettled_nodes;
	
	private UnsettledNode winner;
	
	private List<ProcessingTask> processing_tasks;
	private CyclicBarrier processing_task_barrier;
	private CyclicBarrier reexecute_task_barrier;

	private int leaf_processing_task_count;
	private int processing_split_count;
	
	private class ProcessingTask extends RecursiveAction {
		
		ProcessingTask pt1;
		ProcessingTask pt2;
		
		Map<Vertex, List<Edge>> adjacencies;
		
		Map<Vertex, Integer> distances_from_source;
		Queue<UnsettledNode> unsettled_nodes_queue;
		
		boolean leaf_task = true;
		
		public ProcessingTask(final Map<Vertex, List<Edge>> adjacencies) {
			
			if(processing_split_count < MAX_PROCESSING_SPLIT_COUNT) {
				processing_split_count += 1;
				
				List<Map<Vertex, List<Edge>>> split_adjacencies =
						Graph.splitAdjacencies(adjacencies);
				
				pt1 = new ProcessingTask(split_adjacencies.get(0));
				pt2 = new ProcessingTask(split_adjacencies.get(1));
				
				leaf_task = false;
				
			} else {
				this.adjacencies = adjacencies;
			}
			
			distances_from_source = new HashMap<Vertex, Integer>();
			unsettled_nodes_queue = new PriorityQueue<UnsettledNode>();
			
			if (leaf_task) {
				leaf_processing_task_count += 1;
				
				processing_task_barrier = 
						new CyclicBarrier(leaf_processing_task_count,
								query_tasks_for_winner);
				
				reexecute_task_barrier = 
						new CyclicBarrier(leaf_processing_task_count + 1);
			}
			
			processing_tasks.add(this);
		}
		
		/* ---------------------------------------------------------------- */

		public void setSource(final Vertex source) {
			UnsettledNode us_node = new UnsettledNode(0, source);
			winner = us_node;
			
			settled_nodes.add(source);
		}
		
		private void findWinner() {
			
			/* The CyclicBarrier leaf_barrier waits for all "leaf" tasks to
			 * reach this point before continuing. Once all tasks reach the
			 * barrier, the query_tasks_for_winner Runnable executes once.
			 */
			
			try {
				processing_task_barrier.await();
			} catch (InterruptedException ex) {
				return;
			} catch (BrokenBarrierException ex) {
				return;
			}

			processWinnerAndUnsettledNodes();
		}
		
		private Runnable query_tasks_for_winner = new Runnable() {
			public void run() {
				queryTasksForWinner();
			}
		};
		
		private void queryTasksForWinner() {
			UnsettledNode potential_winner = null;
			
			global_unsettled_nodes.clear();
			
			for (ProcessingTask pt : processing_tasks) {
				UnsettledNode us_node = pt.unsettled_nodes_queue.peek();
				
				if (null == us_node)
					continue;

				if ((null == potential_winner)
						|| (potential_winner.compareTo(us_node) > 0))
					potential_winner = us_node;
				
				/* A bit old fashioned, but I will worry about an optimized way
				 * once this whole thing works.
				 */

				Object temp_node_array[] = pt.unsettled_nodes_queue.toArray();

				for (int i = 0; i < temp_node_array.length; i++)
					global_unsettled_nodes
							.add((UnsettledNode) (temp_node_array[i]));
			}

			winner = potential_winner;
			
			if (null != winner) {
				
//				System.out.println("Winner:" + winner.node.getId() + ","
//						+ winner.distance + "," + System.currentTimeMillis());
				
				settled_nodes.add(winner.node);
			}
		}
		
		private void processWinnerAndUnsettledNodes() {
			if ((null != winner) && (0 == winner.distance)) 
				reset(winner.node);
			else if (winner == unsettled_nodes_queue.peek())
				unsettled_nodes_queue.poll();
			
			if(global_unsettled_nodes != null)
				for(UnsettledNode us_node : global_unsettled_nodes)
					distances_from_source.put(us_node.node, us_node.distance);
		}
		
		private void reset(final Vertex node) {
			unsettled_nodes_queue.clear();
			distances_from_source.clear();
			distances_from_source.put(node, 0);
		}
		
		/* ---------------------------------------------------------------- */
		
		boolean is_active;
		
		@Override
		public void compute() {
			is_active = true;
	
			/* The method is considered to be in the reexecuting state if 
			 * called by a thread outside of the fork/join pool. What we
			 * want to take place in that situation is to fall through to
			 * the cyclic barrier below and trigger the leaf nodes to
			 * run the algorithm.
			 */
			
			boolean reexecuting = !inForkJoinPool();
									
			while (true) {
				
				/* Skip this section if we are reentering the compute() to
				 * trigger another execution.
				 */
				
				if(false == reexecuting) {
					if (leaf_task) {
						
						/* Leaf task */

						processWinnerAndUnsettledNodes();

						while (null != winner) {
							relax(winner.node, winner.distance);
							findWinner();
						}

					} else {

						/* Branch task */
						
						pt1.fork();
						pt2.compute();

						break;
					}
				}
								
				/* Wait for reexecution triggered by calling compute() from
				 * a thread outside the fork/join pool.
				 */
				
				try {
					reexecute_task_barrier.await();
				} catch (InterruptedException ex) {
					return;
				} catch (BrokenBarrierException ex) {
					return;
				}
				
				/* Reexecuting trigger? Return */
				
				if (reexecuting)
					break;
			}
		}

		private void relax(final Vertex node, int dist_to_node) {
			for (Vertex target : getNeighbors(node)) {
				int dist = dist_to_node + getDistance(node, target);
				if (getShortestDistance(target) > dist) {
					
//					System.out.println(getShortestDistance(target) + ">" + dist
//							+ " => " + target + "->" + node);
					
					predecessors.put(target, node);
					distances_from_source.put(target, dist);
					unsettled_nodes_queue.add(new UnsettledNode(dist, target));
				}
			}
		}
		
		private int getShortestDistance(final Vertex destination) {
			Integer d = distances_from_source.get(destination);
			return (d == null) ? Integer.MAX_VALUE : d;
		}

		private int getDistance(final Vertex node, final Vertex target) {
			for (Edge edge : adjacencies.get(node))
				if (edge.getDestination().equals(target))
					return edge.getWeight();

			throw new RuntimeException("Should not happen");
		}

		private List<Vertex> getNeighbors(final Vertex node) {
			List<Vertex> node_neighbors = new ArrayList<Vertex>();
			for (Edge edge : adjacencies.get(node)) {
				Vertex destination = edge.getDestination();
				if (!settled_nodes.contains(destination))
					node_neighbors.add(destination);
			}

			return Collections.unmodifiableList(node_neighbors);
		}
	}
	
	/* -------------------------------------------------------------------- */
	
	private static ForkJoinPool fork_join_pool = 
			dijkstra.resources.Concurrency.getForkJoinPool();
	
	private final List<Vertex> nodes;
	
	private ProcessingTask root_processing_task;

	public DijkstraAlgorithm(final Graph graph) {
		nodes = graph.getVertexes();
		
		settled_nodes = new HashSet<Vertex>();
		predecessors = new ConcurrentHashMap<Vertex, Vertex>();
		global_unsettled_nodes = new ArrayList<UnsettledNode>();
		
		processing_tasks = new ArrayList<ProcessingTask>();
		root_processing_task = new ProcessingTask(graph.getAdjacencies());
	}

	public void execute(final Vertex source) {
	
		settled_nodes.clear();
		predecessors.clear();
				
		if (null != root_processing_task) {
			root_processing_task.setSource(source);
			
			if (false == root_processing_task.is_active) 
				fork_join_pool.execute(root_processing_task);
			else 
				root_processing_task.compute();
			
			while(null != winner) Thread.yield();
		}
	}
	
	public void execute(final int node_num) {
		execute(nodes.get(node_num));
	}
	
	/*
	 * These methods return the path from the source to the selected target and
	 * NULL if no path exists
	 */
	
	public List<Vertex> getPath(final Vertex target) {
		LinkedList<Vertex> path = new LinkedList<Vertex>();
		Vertex step = target;
		// check if a path exists
		if (predecessors.get(step) == null) {
			return null;
		}
		path.add(step);
		while (predecessors.get(step) != null) {
			step = predecessors.get(step);
			path.add(step);
		}
		return Collections.unmodifiableList(path);
	}
	
	public List<Vertex> getPath(final int node_num) {
		return getPath(nodes.get(node_num));
	}
	
	public void terminate() {
		winner = null;
		root_processing_task = null;
		
		for(ProcessingTask pt : processing_tasks)
			pt.cancel(true);
	}
}
