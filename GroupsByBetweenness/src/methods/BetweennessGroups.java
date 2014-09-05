package methods;

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
import dataProcessing.GDFReader;
import dataProcessing.SQLGrabber;
import dataProcessing.ComponentHelper;

public class BetweennessGroups {
	private List<Node> nodeList;
	private List<Edge> edgeList;
	private ComponentHelper ch;
	
	/**
	 * Constructur
	 * @param nodeList List of Nodes of the graph
	 * @param edgeList List of Edges of the graph
	 */
	public BetweennessGroups(List<Node> nodeList,List<Edge> edgeList){
		this.nodeList=nodeList;
		this.edgeList=edgeList;
		this.ch = new ComponentHelper(nodeList,edgeList);
	}
	/**
	 * Repeatedly uses findBetwCommunities to get different sets of communities. Then names matching communities
	 * with the same name
	 * @param numberOfSets Number Of Sets to produce
	 * @param threshold Threshold for the betweenness for the adjusted brandes
	 * @return set of communities
	 */
	public  List<Map<String,List<Node>>> tyler(int numberOfSets,double threshold,long seed,boolean directional){
		List<Map<String,List<Node>>> sets = new ArrayList<Map<String,List<Node>>>();
		for (int i =0;i<numberOfSets;i++){
			nodeList=ch.assignNeighbors(directional);	
			ch.edgesRemoved=0;
			sets.add(findBetwCommunities(threshold,seed+i,directional)); 
			System.out.println("Iteration "+(i+1));										
		}
		//Give matching communities the same name
		for (int i = 0 ; i<sets.size()-1;i++){
			Map<String,List<Node>> set = sets.get(i);
			Map<String,List<Node>> nextSet = sets.get(i+1);
			//communities in the current set
			for (Map.Entry<String, List<Node>> entry : set.entrySet()){
				if (entry.getValue()==null) continue;
				//communities in the next set
				for (Map.Entry<String, List<Node>> nextEntry : nextSet.entrySet()){
					if (nextEntry.getValue()==null) continue;
					List<Node> shared = null;
					boolean same = false; //if they are the same sets
					if (entry.getValue().size()>nextEntry.getValue().size()){
						shared = new ArrayList<Node>(entry.getValue());
						shared.retainAll(nextEntry.getValue());
						if (shared.size()>=Math.ceil(entry.getValue().size()/2.)) same = true;
					} else {
						shared = new ArrayList<Node>(nextEntry.getValue());
						shared.retainAll(entry.getValue());
						if (shared.size()>=Math.ceil(nextEntry.getValue().size()/2.)) same = true;
					}
					
					//When they are the same set rename second set to the first set 
					if (same ){
						if(!entry.getKey().equals(nextEntry.getKey())){
							String tmpString = entry.getKey();//group name
							//save list that occupies groupname space
							List<Node> tmpList = nextSet.get(tmpString);
							//remove group occupying space
							//nextSet.remove(tmpString);
							//Put nextEntry in right space
							nextSet.put(tmpString, nextEntry.getValue());
							//save old position of nextEntry
							tmpString = nextEntry.getKey();
							//remove nextEntry from wrong space
							//extSet.remove(tmpString);
							//put saved entry onto old position from nextentry												
							nextSet.put(tmpString, tmpList);
						}
						//Break inner for loop as similar group is found
						break;
					}					
				}
				
			}
		}
		return sets;
	}
	
	/**
	 * Finds communities based on the algorithm of Typer and all
	 * @param threshold Value for the dijkstra of how much the highest betweenness must be OVER the component size -1 (community criterion) to terminate
	 */
	public  Map<String,List<Node>> findBetwCommunities(double threshold, long seed, boolean directional){
		
		//"Break the graph into connected components"
		//-> Check for compontents that are not connected
		//Start with a Node Check the shortest distances to that node
		//If there are Nodes with infinite they are in another community
		//reduce the list to the ones with infinite distance
		//repeat that for the rest until there are no others
		
		//Bool determines if there are still nodes without groups
		boolean nodesLeft = true;
		//Initialise lift to that has to get splitt
		List<Node> toSplitt= nodeList;
		//Initialize component list that has components of connected nodes
		List<List> components = new ArrayList<List>();
		while (nodesLeft){
			//1. Get all Nodes connected to node 0
			ch.dijkstra(toSplitt.get(0),toSplitt);
			//Put all Nodes with distance infinity in a new list
			Iterator<Node> it = toSplitt.iterator();
			///List with connected nodes
			List<Node> connected = new ArrayList<Node>();
			///List with Unconnected nodes
			List<Node> unconnected = new ArrayList<Node>();
			while (it.hasNext()){
				Node current = it.next();
				if (current.getPrevious()!=null || current.getDistance()==0){
					connected.add(current);
				} else {
					unconnected.add(current);
				}
			}
			//connected Parts form a new component
			if (!connected.isEmpty()) components.add(connected);
			//Check if there are still unconnected components
			if (unconnected.isEmpty()){
				nodesLeft = false;
			}else{
				toSplitt=unconnected;
			}
		}
		
		//"For each component, check to see if component is a community."
		Map<String,List<Node>> communities = new HashMap<String,List<Node>>(); // List of finished communities
		int communityNumber = 1;
		int n = components.size();
		for (int i = 0; i<n;i++){
			List<Node> currentComp = components.get(i);
			// a component of five or less vertices can not be further divided
			if (currentComp.size()<6){
				//if (debug) System.out.println("New Community found through size criterion:\n"+currentComp);
				communities.put(""+communityNumber,currentComp);
				communityNumber++;
				components.remove(currentComp);
				i=i-1; //index is lowered because of removal
				n = components.size();
			}else{ // a component with n vertices has the highest betweenness of n-1 it is a community
				List<Edge> intraCom = new ArrayList<Edge>();
				//When size is greater threshold then a random subset is created for computational ease
				intraCom = componentBetweenness(currentComp,threshold,seed,directional);
				float highestBetweenness = 0;
				for (Edge intraEdge :intraCom){ 
					if (intraEdge.getWeight()>highestBetweenness) highestBetweenness = intraEdge.getWeight();
				}
				//Community Check by leaf betweenness criterium
				if (highestBetweenness <= currentComp.size()-1){
					//if (debug) System.out.println("New Community of size " +currentComp.size()+ " found trough highest betweenness("+highestBetweenness+") criterion:\n"+currentComp);
					communities.put(""+communityNumber,currentComp);
					communityNumber++;
					components.remove(currentComp);
					i=i-1; //index ist lowered because of removal
					n = components.size();
				} else { //Component is no community. Remove edges until graph is split in two components
					//find edges with highest betweenness
					List<Edge> hb = new ArrayList<Edge>();
					for (Edge ce : intraCom){
						if (ce.getWeight()==highestBetweenness) 
							hb.add(ce);
					}
					//randomly pick one of the hb edges to delete
					Random rng = new Random(seed);
					Edge toDelete = hb.get(rng.nextInt(hb.size()));
					
					//remove edge
					intraCom.remove(toDelete);
					nodeList=ch.removeEdge(toDelete);
					System.out.println("\t btwns: "+ highestBetweenness);
					//Check if the edge removal has created two components
					ch.dijkstra(currentComp.get(0),currentComp);
					List<Node> connected = new ArrayList<Node>();
					List<Node> unconnected = new ArrayList<Node>();
					for (Node node:currentComp){
						if (node.getPrevious()!=null || node.getDistance()==0){
							connected.add(node);
						} else {
							unconnected.add(node);
						}
					}
					//Wenn es keine unconnected gibt müssen weitere edges entfernt werden
					if (unconnected.size()==0){
						i=i-1; //Alle schritte füe gleiche Komponente noch ein mal durch gehen.
					} else { // die beiden aufgespalteten Componenten weiter untersuchen
						//if (debug) System.out.println("Splitting components into: \n"+connected+"\n"+unconnected);
						components.remove(currentComp); 
						i=i-1; //index ist lowered because of removal
						components.add(connected);
						components.add(unconnected);
						n = components.size();
					}	
				}
			}
		}

		return communities;
	}
	
	/**
	 * Calculate betweenness for whole component
	 * @param component List of Nodes
	 * @param threshold Value of how much the highest betweenness must be OVER the component size -1 (community criterion) to terminate
	 * @return List of Edges in the component
	 */
	public  List<Edge> componentBetweenness(List<Node> component, double threshold, long seed, boolean directional){
		List<Edge> result = new ArrayList<Edge>();
		//reset weights
		for (Edge current : edgeList){
			current.setWeight(0);
		}
		//Dijkstra for all Nodes calcs betweenness
		if (threshold == Double.POSITIVE_INFINITY){
			for (Node current : component){
				ch.dijkstra(current,component);
				result = ch.getShortestEdges(component, true, result, directional);
			}
		}else { //Dijkstra for as long as threshold is not overdone
			float highestBetweenness = 0;
			List<Node> subset = new ArrayList<Node>();
			Random rng = new Random(seed);
			while (highestBetweenness < threshold+component.size()-1 && subset.size()<component.size()){
				boolean isDrawn = false;				
					while (!isDrawn){
					Node drawn = component.get(rng.nextInt(component.size()));
					if (!subset.contains(drawn)){ 
						subset.add(drawn);
						isDrawn=true;
					}
				}			
				for (Edge current : result){
					current.setWeight(0);
				}	
				for (Node current : subset){
					ch.dijkstra(current,subset);
					result = ch.getShortestEdges(subset, true, result, directional);
				}
				for (Edge intraEdge :result){ 
					if (intraEdge.getWeight()>highestBetweenness) highestBetweenness = intraEdge.getWeight();
				}
			}
		}
		return result;
	}		
}
