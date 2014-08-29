package methods;

import java.util.List;

import data.Edge;
import data.Node;
import dataProcessing.ComponentHelper;

public class Centrality {
	private List<Node> nodeList;
	private List<Edge> edgeList;
	private ComponentHelper ch;
	
	public Centrality(List<Node> nodeList,List<Edge> edgeList){
		this.edgeList=edgeList;
		this.nodeList=nodeList;
		ch = new ComponentHelper(nodeList,edgeList);
	}
	
	/**
	 * Calculates the degree centrality of the node
	 * @param node named Node
	 * @param directed if the network has directed relations. If so only the outgoing relations are calculated.
	 * @return degree Centrality
	 */
	public float degreeCentrality(Node node, boolean directional){
		float centrality = 0;
		this.nodeList=ch.assignNeighbors();
		//go through all edges and see if it is ingoing or outgoing for the node
		for (Edge edge : edgeList){
			if ( (edge.getSource().equals(node))
					||(!directional && edge.getTarget().equals(node))){				
				centrality++;
			}
		}
		//centrality is normalized by n-1
		return centrality/(nodeList.size()-1);
	}
	
	/**
	 * Calculates the closeness centrality of the node
	 * @param node named Node
	 * @param directional if the network has directed relations. If so only the outgoing relations are calculated.
	 * @return closeness centrality
	 */
	public float closenessCentrality (Node node, boolean directional){
		float centrality = 0;
		//calculate dijkstra once as it the source node always stays the same
		ch.dijkstra(node, nodeList);
		//get the shortest path for all nodes to the source node
		for (Node current:nodeList){
			centrality+=ch.shortestPathLength(node, current, directional, false);
		}
		//get number of reachable nodes from the node
		int numReachableNodes = 0;
		for (Node current : nodeList){
			if (current.getPrevious()!=null)
				numReachableNodes++;
		}
		
		//normalized by the number of reachable nodes and the number of nodes in the network
		return (numReachableNodes/(nodeList.size()-1))/(centrality/numReachableNodes);
	}
	
	public float betweennessCentrality (Node node, boolean directional){
		float centrality = 0;
		
		return centrality;
	}

}
