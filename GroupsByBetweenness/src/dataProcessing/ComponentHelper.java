package dataProcessing;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import data.Edge;
import data.Node;

public class ComponentHelper {
	private List<Node> nodeList;
	private List<Edge> edgeList;
	public int edgesRemoved;
	
	/**
	 * Constructor
	 * @param nodeList List of Nodes in the Graph
	 * @param edgeList List of Edges in the Graph
	 */
	public ComponentHelper(List<Node> nodeList,List<Edge> edgeList){
		this.nodeList=nodeList;
		this.edgeList=edgeList;
		this.edgesRemoved=0;
	}
	
	/**
	 * Writes the Nodes from the Sets to a File. Each Note has a value for how often it is contained in a certain community.
	 * @param sets Set of sets of communities of Nodes
	 * @param path Save path of the file
	 */
	public void writeGroupsToFile(List<Map<String,List<Node>>> sets,String path){
		//List of all nodes with Map of number of times the note is in a certain community
		Map<Node,Map<String,Integer>> nodesCommunities = new HashMap<Node,Map<String,Integer>>();
		
		for (Map<String,List<Node>> set : sets){
			for (String communityName :set.keySet()){
				List<Node> community = set.get(communityName);
				if (community != null)
					for (Node node :community ){
						//If the node is new to the map
						if(nodesCommunities.get(node)==null){
							nodesCommunities.put(node, new HashMap<String,Integer>());						
						}
						
						//raise how often the node is within the community
						int timesInCommunity = 0;
						
						if (nodesCommunities.get(node).get(communityName)!=null) 
							timesInCommunity = nodesCommunities.get(node).get(communityName);
						
						nodesCommunities.get(node).put(communityName, timesInCommunity+1);
					}
			}
		}
		
		//Write list to Text File
		try{
			FileWriter write = new FileWriter(path,false);
			PrintWriter printLine = new PrintWriter(write);
			printLine.println("sep=\t");
			printLine.println("Name"+"\t"+"Community"+"\t"+"Times in Community");
			for (Node node : nodesCommunities.keySet()){
				Map<String,Integer> comms = nodesCommunities.get(node);
				printLine.print(node.getLabel());
				for (String commName : comms.keySet()){
					printLine.println("\t"+commName+"\t"+comms.get(commName));
				}
			}
			
			printLine.close();
		} catch (IOException e){
			e.printStackTrace();
		}
		System.out.println("Wrote communities to *.csv file: "+path);
	}
	
	public void writeStringToFile(String text,String path){
		try{
			FileWriter write = new FileWriter(path,false);
			PrintWriter printLine = new PrintWriter(write);
			printLine.println(text);			
			printLine.close();
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Removes both neighbor pointers from the source and target of the edge
	 * @param edge Edge 
	 */
	public  List<Node> removeEdge(Edge edge){
		Node source = null;
		Node target = null;
		edgesRemoved++;
		for (Node node : nodeList){
			if(node.getId().equals(edge.getSource())) source = node;
			else if (node.getId().equals(edge.getTarget())) target = node;
		}
		if (source != null)	source.getNeighbors().remove(target);
		if (target != null) target.getNeighbors().remove(source);
		System.out.print(edgesRemoved+":\t"+source.getId()+"\t\t-\t"+target.getId());
		return nodeList;		
	}
	
	/**
	 * Returns edge from source to target
	 * @param source ID of the source Node
	 * @param target ID of the target Node
	 * @param direction if not direction edge will also be weigthed if it goes into the other direction
	 * @return
	 */
	public  Edge getEdge(String source, String target, boolean directional){
		Iterator<Edge> it = edgeList.iterator();
		while (it.hasNext()){
			Edge edge = it.next();
			if ( (edge.getSource().equals(source) && edge.getTarget().equals(target))
					||(!directional && edge.getSource().equals(target) && edge.getTarget().equals(source))){				
				return edge;
			}
		}
		return null;
	}
	

	
	/**
	 * Adds a weight to an edge
	 * @param source ID of the source Node
	 * @param target ID of the target Node
	 * @param direction if not direction edge will also be weigthed if it goes into the other direction
	 * @return
	 */
	public  Edge addWeight(String source,String target, boolean directional){
		Edge edge = getEdge(source, target, directional);
		if (edge == null) System.out.println("Did you do a directional analysis on a nondirectional network? Null pointer exception in 3..2..1..");
		edge.setWeight(edge.getWeight()+0.5f); //0.5 because in the end it would be devided by two
		return edge;
	}
	
	/**
	 * Goes through all edges and sets the neighbors for the nodes
	 */
	public  List<Node> assignNeighbors(boolean directional){ 

		//go through all edges
		for (Edge edge : edgeList){
			Node source = null; 
			Node target = null;
			Iterator<Node> jt = nodeList.iterator();
			//For each edge go through all nodes
			for (Node node : nodeList){
				if (edge.getSource().equals(node.getId())){
					source=node;
				} else if (edge.getTarget().equals(node.getId())){
					target=node;
				}
				//if both nodes are there we do not have to continue
				if(source != null && target != null)
						break;
			}
			//set source and target as neighbors
			if (source != null && target !=null){
				source.addNeighbor(target);
				if (!directional)
					target.addNeighbor(source);
			}
		}
		return nodeList;
	}
	
	/**
	 * Calculates for all other notes the shortest distance to the source node and sets their distance to it. Then sets
	 * the predecessing node to the current node which leads to the shortest path. Also adds weights to the notes
	 * according to how often they are part of a shortest path. ATTENTION: Do not forget to assign neighbors befor the first usage of Dijkstra
	 * @param source Node to which all smallest distances should be calculated.
	 * @param list List of the nodes for which the dijkstra has to be done
	 * @param calcBetweenness if this is true the edge weights are added 0.5 for each traverse
	 */
	public  void dijkstra(Node source, List<Node> list){
		if (!list.contains(source)){
			System.out.println("dijstra: source must be contained in List");
			return;
		}
		//Reset Nodes
		Iterator<Node> it = list.iterator();
		while (it.hasNext()){
			Node n = it.next();
			n.setDistance(Double.POSITIVE_INFINITY);
			n.voidPrevious();
		}
		//Distance for source is zero-> is the first one chosen
        source.setDistance(0.);
        //List of unvisited nodes
        PriorityQueue<Node> unvisited = new PriorityQueue<Node>();
        Node current = source;
        unvisited.add(source);
        while (!unvisited.isEmpty()){
        	//Get Node with the smallest distance
        	current = unvisited.poll();
        	
	        //Calculate new distances
        	if (list.contains(current))
		        for (Node neighbor : current.getNeighbors()){
	        		if (current.getDistance() +1 <= neighbor.getDistance()){
	        			//Set new distance
	        			neighbor.setDistance(current.getDistance()+1);
	        			//add neighbor to unvisited
	        			if (!unvisited.contains(neighbor)){
	        				
	        				unvisited.add(neighbor); 
	        				
	        			}
	        			
	        			neighbor.addPrevious(current);
	        		}	        	
		        }	        
        }
	}
	
	/**
	 * return the edges of the shortest paths of a List of Nodes. !!!Dijkstra must be done first and use the same list!!!!
	 * @param list list of Nodes for which the edges should be returned
	 * @param calcBetweenness if the betweenness for the edges should be calculated
	 * @param result An array List of edges for the method to work with. The new edges are added to the list if they are not already in there
	 * @param directional if the network is directional
	 * @return
	 */
	public List<Edge> getShortestEdges(List<Node> list, boolean calcBetweenness, List<Edge> result, boolean directional, long seed){		
		for (Node akt : list){
			if (akt.getPrevious()==null){
				continue;
			}
			Node i = akt;
			while (i.getPrevious()!=null){
				//Randomly draw one of the predecessors
				Random rng = new Random(seed);
				int prev = rng.nextInt(i.getPrevious().size());
				
				if (calcBetweenness){
					Edge toAdd = addWeight(i.getId(),i.getPrevious().get(prev).getId(),directional);
					if (!result.contains(toAdd))
						result.add(toAdd);
				}
				else{
					Edge toAdd = getEdge(i.getId(),i.getPrevious().get(prev).getId(),directional);
					if (!result.contains(toAdd))
						result.add(toAdd);
				}
			
				i=i.getPrevious().get(prev);
			}			
		}
		return result;
	}
	
	/**
	 * Return the edges of a shortest path of a node to the node for which the dijkstra was done before !!!Dont forget to do the dijkstra first!!
	 * @param list List of Nodes 
	 * @param calcBetweenness if the edges should get the betweenness calculated
	 * @param result list of edges where the result should be added to
	 * @param directional if the network is directional
	 * @param from the source node
	 * @param seed seed for the random drawing of one of the predecessors, if there are multiple shortest paths
	 * @return
	 */
	public List<Edge> getShortestEdges(List<Node> list, boolean calcBetweenness, List<Edge> result, boolean directional, Node from, long seed){
		if (!list.contains(from))
			return result;
		Node current = from;
		while (current.getPrevious()!=null){
			//Randomly draw one of the predecessors
			Random rng = new Random(seed);
			int prev = rng.nextInt(current.getPrevious().size());
			
			if (calcBetweenness){
				Edge toAdd = addWeight(current.getId(),current.getPrevious().get(0).getId(),directional);
				if (!result.contains(toAdd))
					result.add(toAdd);
			}
			else{
				Edge toAdd = getEdge(current.getId(),current.getPrevious().get(0).getId(),directional);
				if (!result.contains(toAdd))
					result.add(toAdd);
			}
			
			
			current=current.getPrevious().get(prev);
		}
		
		return result;
	}
	
	/**
	 * Gets the number of shortests path from a node to another node that contain a third node
	 * @param from Start Node
	 * @param to Goal Node
	 * @param containing Node that should be contained in the path. Set to null if there is no specific node
	 * @param directional If the network is directional
	 * @return array of shortest paths. [0] contains the node [1] does not
	 */
	public int[] getNumberOfShortestPaths(Node from,Node to,Node containing,List<Node> connected,boolean directional){		
		dijkstra(from,connected);
		int[] number = pathRecurator(to,containing,(containing==null)?true:false);//If containing is null found is set to true because there is no specific node that should be in the path
		return number;
	}
	
	/**
	 * Recursive helper to calculate the number of shortests paths to a node
	 * @param node goal node
	 * @param containing node that should be contained in the path
	 * @param found if the node was in the path already
	 * @return number of shortest paths from this node to goal node. [0] contains the node [1] does not
	 */
	private int[] pathRecurator(Node from, Node containing, boolean found){
		int[] number = {0,0};
		//Check if this node is the one searched for
		if (from == containing)
			found=true;
		//go through all neighbors
		for (Node prev : from.getPrevious()){
			if (prev.getPrevious() == null) //reached the end
				if (found) //is only a valuable path if node is contained
					return new int[] {1,0};
				else 
					return new int[] {0,1};			
			else {
				int[] intermediate = pathRecurator(prev,containing,found);//go through all neighbors for the next node
				number[0] += intermediate[0]; 
				number[1] += intermediate[1];
			}
		}
			
		return number;
	}
}
