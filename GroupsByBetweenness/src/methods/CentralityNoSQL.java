package methods;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.Edge;
import data.Node;
import dataProcessing.ComponentHelperNoSQL;

public class CentralityNoSQL {
	private Map<String,Node> nodeList;
	//private List<Edge> edgeList;
	private ComponentHelperNoSQL ch;
	
	public CentralityNoSQL(Map<String,Node> nodeList,Map<String,Edge> edgeList){
		//this.edgeList=edgeList;
		this.nodeList=nodeList;
		ch = new ComponentHelperNoSQL(nodeList,edgeList);
	}
	
	/**
	 * Calculates the degree centrality of the node. "Directed" is determined by assign neighbors
	 * @param node named Node
	 * @return degree Centrality
	 */
	public float degreeCentrality(Node node){
		float centrality = 0;
		//this.nodeList=ch.assignNeighbors();
		//go through all edges and see if it is ingoing or outgoing for the node
		centrality+=node.getNeighbors().size();
		//centrality is normalized by n-1
		return centrality/(nodeList.size()-1);
	}
	
	/**
	 * Calculates the closeness centrality of the node. Dont forget to assign neigbors beforehand. Directional is determined by assign neighbors
	 * @param node named Node
	 * @return closeness centrality
	 */
	public float closenessCentrality (Node node,boolean directional){
		float centrality = 0;
		ch.dijkstra(node,new HashMap<String,Node>());
		//get the shortest path for all nodes to the source node
		for (Node current:nodeList.values()){
			if (current.getDistance()<Double.POSITIVE_INFINITY)
				centrality+=current.getDistance();
		}
		//get number of reachable nodes from the node
		int numReachableNodes = 0;
		for (Node current : nodeList.values()){
			if (current.getPrevious()!=null)
				numReachableNodes++;
		}
		
		//normalized by the number of reachable nodes and the number of nodes in the network
		float a = (float) numReachableNodes / (float) (nodeList.size()-1) ;
		float b = (float) centrality / (float) numReachableNodes;
		float c = a / b;
		return c ;
	}
	
	/**
	 * Calculates the betweenness centrality of a node
	 * @param node Node for which the centrality should be calculated
	 * @param directional If the network has directional relations
	 * @return betweenness centrality
	 */
	public float betweennessCentrality (Node node,boolean directional){
		float sum = 0; //sum of shortests paths containing the node divided by the sum of shortest paths
		//get list of connected Nodes
		Node any = nodeList.values().iterator().next();
		Map<String,Node> connected =ch.dijkstra(any,new HashMap<String,Node>());
		//loop to calculate the sum
		for (int j = 0;j<connected.size()-1;j++){
			Node jNode = connected.get(j);
			if (node == jNode) continue;
			for (int k = j+1;k<connected.size();k++){
				Node kNode = connected.get(k);
				if (node == jNode) continue;
				int shortestPaths[] = ch.getNumberOfShortestPaths(jNode, kNode, node,new ArrayList<Node>(connected.values()),directional);
				sum += (float) shortestPaths[0]/ (float) shortestPaths[1];
			}
		}
		
		return (float) sum/((float)(connected.size()-1)*(float)(connected.size()-2)/(float)2);
	}

}