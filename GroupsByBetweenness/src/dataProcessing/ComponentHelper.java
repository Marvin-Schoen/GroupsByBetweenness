package dataProcessing;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import data.Edges;
import data.Nodes;

public class ComponentHelper {
	private List<Nodes> nodeList;
	private List<Edges> edgeList;
	
	/**
	 * Constructor
	 * @param nodeList List of Nodes in the Graph
	 * @param edgeList List of Edges in the Graph
	 */
	public ComponentHelper(List<Nodes> nodeList,List<Edges> edgeList){
		this.nodeList=nodeList;
		this.edgeList=edgeList;
	}
	
	/**
	 * Writes the Nodes from the Sets to a File. Each Note has a value for how often it is contained in a certain community.
	 * @param sets Set of sets of communities of Nodes
	 * @param path Save path of the file
	 */
	public void writeGroupsToFile(List<Map<String,List<Nodes>>> sets,String path){
		//List of all nodes with Map of number of times the note is in a certain community
		Map<Nodes,Map<String,Integer>> nodesCommunities = new HashMap<Nodes,Map<String,Integer>>();
		
		for (Map<String,List<Nodes>> set : sets){
			for (String communityName :set.keySet()){
				List<Nodes> community = set.get(communityName);
				for (Nodes node :community ){
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
			
			printLine.println("Name"+"\t"+"Community"+"\t"+"Times in Community");
			for (Nodes node : nodesCommunities.keySet()){
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
	}
	
	/**
	 * Removes both neighbor pointers from the source and target of the edge
	 * @param edge Edge 
	 */
	public  List<Nodes> removeEdge(Edges edge){
		Nodes source = null;
		Nodes target = null;
		for (Nodes node : nodeList){
			if(node.getId().equals(edge.getSource())) source = node;
			else if (node.getId().equals(edge.getTarget())) target = node;
		}
		if (source != null)	source.getNeighbors().remove(target);
		if (target != null) target.getNeighbors().remove(source);
		return nodeList;		
	}
	
	/**
	 * Returns edge from source to target
	 * @param source ID of the source Node
	 * @param target ID of the target Node
	 * @param direction if not direction edge will also be weigthed if it goes into the other direction
	 * @return
	 */
	public  Edges getEdge(String source, String target, boolean directional){
		Iterator<Edges> it = edgeList.iterator();
		while (it.hasNext()){
			Edges edge = it.next();
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
	public  Edges addWeight(String source,String target, boolean directional){
		Edges edge = getEdge(source, target, directional);
		edge.setWeight(edge.getWeight()+0.5f); //0.5 because in the end it would be devided by two
		return edge;
	}
	
	/**
	 * Goes through all edges and sets the neighbors for the nodes
	 */
	public  List<Nodes> assignNeighbors(){
		Iterator<Edges> it = edgeList.iterator();
		//go through all edges
		while (it.hasNext()){
			Edges edge = it.next();
			Nodes source = null;
			Nodes target = null;
			Iterator<Nodes> jt = nodeList.iterator();
			//For each edge go through all nodes
			while (jt.hasNext()){
				Nodes node = jt.next();
				if (edge.getSource().equals(node.getId())){
					source=node;
				} else if (edge.getTarget().equals(node.getId())){
					target=node;
				}
			}
			//set source and target as neighbors
			if (source != null && target !=null){
				source.addNeighbor(target);
				target.addNeighbor(source);
			}
		}
		return nodeList;
	}
}
