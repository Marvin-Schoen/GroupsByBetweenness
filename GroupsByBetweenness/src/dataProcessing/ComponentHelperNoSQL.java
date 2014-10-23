package dataProcessing;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import data.Edge;
import data.Node;

public class ComponentHelperNoSQL {
	private Map<String,Node> nodeList;
	private Map<String,Edge> edgeList;
	public int edgesRemoved;
	private ArrayList<Node> toReset; // list of nodes that got to be reset for the dijkstra
	
	/**
	 * Constructor
	 * @param nodeList List of Nodes in the Graph
	 * @param edgeList List of Edges in the Graph
	 */
	public ComponentHelperNoSQL(Map<String,Node> nodeList,Map<String,Edge> edgeList){
		this.nodeList=nodeList;
		this.edgeList=edgeList;
		this.edgesRemoved=0;
		toReset=new ArrayList<Node>();
	}
	
	/**
	 * Writes the Nodes from the Sets to a File. Each Note has a value for how often it is contained in a certain community.
	 * @param sets Set of sets of communities of Nodes
	 * @param path Save path of the file
	 */
	public void writeGroupsToFile(List<Map<String,List<Node>>> sets,String path){
		//List of all nodes with Map of number of times the note is in a certain community
		Map<Node,Map<String,Integer>> nodesCommunities = new HashMap<Node,Map<String,Integer>>();
		
		//go thourgh all sets of communities
		for (Map<String,List<Node>> set : sets){
			//go through all communities of the set
			for (String communityName :set.keySet()){
				List<Node> community = set.get(communityName);
				if (community != null)
					//Go through all actors of the community
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
	public  Map<String,Node> removeEdge(Edge edge){
		Node source = nodeList.get(edge.getSource());
		Node target = nodeList.get(edge.getTarget());
		edgesRemoved++;
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
		String edgeKey=source+","+target;
		if (edgeList.containsKey(edgeKey))
			return edgeList.get(edgeKey);
		else if (!directional){
			edgeKey=target+","+source;
			return edgeList.get(edgeKey);
		} else return null;
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
	public  Map<String,Node> assignNeighbors(boolean directional){ 
		//Reset neighbors
		for (Node node : nodeList.values()){
			node.setNeighbors(new ArrayList<Node>());
		}
		//go through all edges
		int i=0; //performance test
		for (Edge edge : edgeList.values()){
			i++;
			Node source = nodeList.get(edge.getSource());
			Node target = nodeList.get(edge.getTarget());
			if (source != null && target !=null){
				source.addNeighbor(target);
				if (!directional)
					target.addNeighbor(source);
			}
		}
		return nodeList;
	}
	
	/**
	 * Helper method to get all nodes with a distance smaller than "compare" from a list
	 * @param compare value
	 * @return I have no Idea. This is used in Collections2.filter(unfiltered,predicate)
	 */
	private Predicate<Node> distanceSmallerThan(final double compare){
		return new Predicate<Node>(){
			public boolean apply(Node node){
				return node.getDistance()<compare;
			}
		};
	}
	
	/**
	 * Calculates for all other notes the shortest distance to the source node and sets their distance to it. Then sets
	 * the predecessing node to the current node which leads to the shortest path. Also adds weights to the notes
	 * according to how often they are part of a shortest path. 
	 * ATTENTION: Do not forget to assign neighbors before the first usage of Dijkstra
	 * @param source Node to which all smallest distances should be calculated.
	 * @param unconnected List of the nodes for which the dijkstra has to be done, to check if they are connected
	 * @param toReset a list of nodes that have to be reset before the next dijkstra
	 * @return [0]list of nodes connected to source. [1]a list of nodes that have to be reset before the next dijkstra
	 * 
	 */
	public  Map<String,Node> dijkstra(Node source, Map<String,Node> unconnected){
		//Collection<Node> toReset = Collections2.filter(nodeList.values(), distanceSmallerThan(Double.POSITIVE_INFINITY));

		// Nodes to be Reset 
		for (Node n : toReset){
			n.setDistance(Double.POSITIVE_INFINITY);
			n.reset();
		}
		toReset = new ArrayList<Node>();
		//connected nodes
		Map<String,Node> connected = new HashMap<String,Node>();
		//Distance for source is zero-> is the first one chosen
        source.setDistance(0.);
        //List of unvisited nodes
        PriorityQueue<Node> unvisited = new PriorityQueue<Node>();
        Node current = source;
        unvisited.add(source);
        toReset.add(source);
        while (!unvisited.isEmpty()){
        	//Get Node with the smallest distance
        	current = unvisited.poll();
        	//current node is connected to the rest        				
        	if (!connected.containsKey(current.getId())){
        		connected.put(current.getId(),current);
        		unconnected.remove(current.getId());
        	} else {
        		int blub = 2;
        	}
	        //Calculate new distances
        	//if (list.contains(current)) //TODO the current node actually must not be contained in the list. The List is just the collection of start and
	        for (Node neighbor : current.getNeighbors()){
        		if (current.getDistance() +1 <= neighbor.getDistance()){
        			//Set new distance
        			neighbor.setDistance(current.getDistance()+1);
        			toReset.add(neighbor);//all neighbors that are change must be reseted afterwards
        			//add neighbor to unvisited
        			if (!neighbor.isVisited()){
        				
        				unvisited.add(neighbor); 
        				neighbor.setVisited(true);
        				
        			}
        			if (!neighbor.getPrevious().contains(current))
        				neighbor.addPrevious(current);
        		}	        	
	        }
	        
        }
        return connected;
	}
	
	/**
	 * return the edges of the shortest paths of a List of Nodes. !!!Dijkstra must be done first and use the same list!!!!
	 * @param list list of Nodes for which the edges should be returned
	 * @param calcBetweenness if the betweenness for the edges should be calculated
	 * @param result An array List of edges for the method to work with. The new edges are added to the list if they are not already in there
	 * @param directional if the network is directional
	 * @return
	 */
	public Map<String,Edge> getShortestEdges(List<Node> list, boolean calcBetweenness, Map<String,Edge> result, boolean directional, long seed){		
		for (Node akt : list){
			if (akt.getPrevious()==null){
				continue;
			}
			Node i = akt;
			while (i.getPrevious().size()>0){
				//Randomly draw one of the predecessors
				Random rng = new Random(seed);
				int prev = rng.nextInt(i.getPrevious().size());
				
				if (calcBetweenness){
					Edge toAdd = addWeight(i.getId(),i.getPrevious().get(prev).getId(),directional);
					if (!result.containsKey(toAdd)){
						String id = toAdd.getSource()+","+toAdd.getTarget();
						result.put(id,toAdd);
					}
				}
				else{
					Edge toAdd = getEdge(i.getId(),i.getPrevious().get(prev).getId(),directional);
					if (!result.containsKey(toAdd)){
						String id = toAdd.getSource()+","+toAdd.getTarget();
						result.put(id,toAdd);
					}
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
	public Map<String,Edge> getShortestEdges(List<Node> list, boolean calcBetweenness, Map<String,Edge> result, boolean directional, Node from, long seed){
		if (!list.contains(from))
			return result;
		Node current = from;
		while (current.getPrevious().size()>0){
			//Randomly draw one of the predecessors
			Random rng = new Random(seed);
			int prev = rng.nextInt(current.getPrevious().size());
			
			if (calcBetweenness){
				Edge toAdd = addWeight(current.getId(),current.getPrevious().get(0).getId(),directional);
				if (!result.containsKey(toAdd)){
					String id = toAdd.getSource()+","+toAdd.getTarget();
					result.put(id,toAdd);
				}
			}
			else{
				Edge toAdd = getEdge(current.getId(),current.getPrevious().get(0).getId(),directional);
				if (!result.containsKey(toAdd)){
					String id = toAdd.getSource()+","+toAdd.getTarget();
					result.put(id,toAdd);
				}
			}
			
			
			current=current.getPrevious().get(prev);
		}
		
		return result;
	}
	
	/**
	 * removes double edges from edgeList. Meaning it removes the edge in another direction
	 */
	public void removeDoubleEdges(){
		//for each edge check if there is an edge in the other direction
		for (Iterator<Map.Entry<String, Edge>> it = edgeList.entrySet().iterator() ;it.hasNext();){
			Map.Entry<String, Edge> entry = it.next();
			String[] nodes = entry.getKey().split(",");
			String reverse = nodes[1]+","+nodes[0]; //the edge the other way around
			if (edgeList.containsKey(reverse))
				it.remove(); //it does not matter which of the both keys to remove
		}
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
		dijkstra(from,new HashMap<String,Node>());
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