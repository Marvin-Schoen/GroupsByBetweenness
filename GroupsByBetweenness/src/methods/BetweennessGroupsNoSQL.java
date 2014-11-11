package methods;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import data.Edge;
import data.Node;
import dataProcessing.ComponentHelperNoSQL;

public class BetweennessGroupsNoSQL {
	private Map<String,Node> nodeList;
	private Map<String,Edge> edgeList;
	private ComponentHelperNoSQL ch;
	
	/**
	 * Constructur
	 * @param nodeList List of Nodes of the graph
	 * @param edgeList List of Edges of the graph
	 */
	public BetweennessGroupsNoSQL(Map<String,Node> nodeList,Map<String,Edge> edgeList){
		this.nodeList=nodeList;
		this.edgeList=edgeList;
		this.ch = new ComponentHelperNoSQL(nodeList,edgeList);
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
		ch.removeDoubleEdges();
		//Write Component to File
		nodeList=ch.assignNeighbors(directional);	
		List<Map<String,Node>> components = findComponents();
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
		Date dNow = new Date( );
		ch.writeCompStatsToFile(components, "C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"components.csv");
		
		for (int i =0;i<numberOfSets;i++){
			nodeList=ch.assignNeighbors(directional);	
			ch.edgesRemoved=0;			
			sets.add(findBetwCommunities(components,threshold,seed+i,directional)); 
			System.out.println("Iteration "+(i+1));										
		}
		//Give matching communities the same name
		return equalComNames(sets);
	}
	
	public List<Map<String,List<Node>>> equalComNames(List<Map<String,List<Node>>> sets){		
		for (int i = 0 ; i<sets.size()-1;i++){
			Map<String,List<Node>> set = sets.get(i);
			Map<String,List<Node>> nextSet = sets.get(i+1);
			//communities in the current set
			for (Map.Entry<String, List<Node>> entry : set.entrySet()){
				if (entry.getValue()==null) continue;
				int bestmatch = 0; //number of matches of the best match
				Map.Entry<String, List<Node>> bestmatchList=null; //List that has the most matches from nextSet
				
				//communities in the next set
				for (Map.Entry<String, List<Node>> nextEntry : nextSet.entrySet()){
					if (nextEntry.getValue()==null) continue;
					List<Node> shared = null;
					if (entry.getValue().size()>nextEntry.getValue().size()){
						shared = new ArrayList<Node>(entry.getValue());
						//shared.retainAll(nextEntry.getValue()); //geht nur wenn alle 2 gleichen nodes auch das gleiche Object sind
						for (Iterator<Node> it = shared.iterator();it.hasNext();){
							Node ns = it.next();
							boolean contained = false;
							for (Node nn : nextEntry.getValue()){
								if(ns.getId().equals(nn.getId()))
									contained = true;
							}
							if (!contained)
								it.remove();
						}
						
						if (shared.size()>bestmatch){
							bestmatch = shared.size();
							bestmatchList = nextEntry;
						}
					} else {
						shared = new ArrayList<Node>(nextEntry.getValue());
						//shared.retainAll(entry.getValue()); //geht nur wenn alle 2 gleichen nodes auch das gleiche Object sind
						for (Iterator<Node> it = shared.iterator();it.hasNext();){
							Node ns = it.next();
							boolean contained = false;
							for (Node nn : entry.getValue()){
								if(ns.getId().equals(nn.getId()))
									contained = true;
							}
							if (!contained)
								it.remove();
						}
						
						if (shared.size()>bestmatch){
							bestmatch = shared.size();
							bestmatchList = nextEntry;
						}	
					}
					if (entry.getKey().equals("602") && bestmatch==120){
						int halt;
						halt =1;
					} 
				}
				//When they are the same set rename second set to the first set 

				if(bestmatch>1){
					System.out.println(entry.getKey()+" has "+bestmatch+" matches with "+bestmatchList.getKey());
					if( !entry.getKey().equals(bestmatchList.getKey())){
					String tmpString = entry.getKey();//group name
					//save list that occupies groupname space
					List<Node> tmpList = nextSet.get(tmpString);
					//Put bestmatchList in right space
					nextSet.put(tmpString, bestmatchList.getValue());
					//save old position of nextEntry
					tmpString = bestmatchList.getKey();
					//put saved entry onto old position from nextentry												
					nextSet.put(tmpString, tmpList);
					//bestmatchlist key has to be altered?
					}
				}
					
				
			}
		}
		return sets;
	}
	
	public List<Map<String,Node>> findComponents(){
		//Initialize component list that has components of connected nodes
		List<Map<String,Node>> components = new ArrayList<Map<String,Node>>();
		//"Break the graph into connected components"
		//-> Check for compontents that are not connected
		//Start with a Node Check the shortest distances to that node
		//If there are Nodes with infinite they are in another community
		//reduce the list to the ones with infinite distance
		//repeat that for the rest until there are no others
		
		//Bool determines if there are still nodes without groups
		boolean nodesLeft = true;//
		System.out.println("----");
		//Initialise lift to that has to get splitt
		Map<String,Node> toSplitt= new HashMap<String,Node>(nodeList);
		
		while (nodesLeft){
			//1. Get all Nodes connected to node 0
			Node any = toSplitt.values().iterator().next(); //Keep in mind that with a directed network components 
								//long beforeDijkstra=System.currentTimeMillis();
			Map<String,Node> connected=ch.dijkstra(any,toSplitt);
								//System.out.println("Dijkstra to find a component took "+(System.currentTimeMillis()-beforeDijkstra)+" milliseconds to find component of size " + connected.values().size()+".\t"+toSplitt.size()+" to go.");
			//Put all Nodes with distance infinity in a new list
			///List with connected nodes
			//connected Parts form a new component
			if (!connected.isEmpty()) 
				components.add(connected);
			//Check if there are still unconnected components
			if (toSplitt.isEmpty())
				nodesLeft = false;
		}
		return components;
	}
	
	
	/**
	 * Finds communities based on the algorithm of Typer and all
	 * @param threshold Value for the dijkstra of how much the highest betweenness must be OVER the component size -1 (community criterion) to terminate
	 */
	public  Map<String,List<Node>> findBetwCommunities(List<Map<String,Node>> comp, double threshold, long seed, boolean directional){
		
		//Copy comp, as we dont want to change it. It is needed for later iterations
		List<Map<String,Node>> components = new ArrayList<Map<String,Node>>(comp);
		
		//"For each component, check to see if component is a community."
		Map<String,List<Node>> communities = new HashMap<String,List<Node>>(); // List of finished communities
		int communityNumber = 1;
		int n = components.size();
		for (int i = 0; i<n;i++){
			Map<String,Node> currentComp = components.get(i);
			// a component of five or less vertices can not be further divided
			if (currentComp.size()<6){
				//if (debug) System.out.println("New Community found through size criterion:\n"+currentComp);
				communities.put(""+communityNumber,new ArrayList<Node>(currentComp.values()));
				communityNumber++;
				components.remove(currentComp);
				i=i-1; //index is lowered because of removal
				n = components.size();
			}else{ // a component with n vertices has the highest betweenness of n-1 it is a community
				Map<String,Edge> intraCom = new HashMap<String,Edge>();
				//When size is greater threshold then a random subset is created for computational ease
				intraCom = componentBetweenness(new ArrayList<Node>(currentComp.values()),threshold,seed,directional);
				float highestBetweenness = 0;
				for (Edge intraEdge :intraCom.values()){ 
					if (intraEdge.getWeight()>highestBetweenness) highestBetweenness = intraEdge.getWeight();
				}
				//Community Check by leaf betweenness criterium
				if (highestBetweenness <= currentComp.size()-1){
					//if (debug) System.out.println("New Community of size " +currentComp.size()+ " found trough highest betweenness("+highestBetweenness+") criterion:\n"+currentComp);
					communities.put(""+communityNumber,new ArrayList<Node>(currentComp.values()));
					communityNumber++;
					components.remove(currentComp);
					i=i-1; //index ist lowered because of removal
					n = components.size();
				} else { //Component is no community. Remove edges until graph is split in two components
					//find edges with highest betweenness
					List<Edge> hb = new ArrayList<Edge>();
					for (Edge ce : intraCom.values()){
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
					Map<String,Node> unconnected = new HashMap<String,Node>(currentComp);
					
					Node any = currentComp.values().iterator().next();
					Map<String, Node> connected = ch.dijkstra(any,unconnected);
					//Wenn es keine unconnected gibt m�ssen weitere edges entfernt werden
					if (unconnected.size()==0){
						i=i-1; //Alle schritte f�e gleiche Komponente noch ein mal durch gehen.
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
	public  Map<String,Edge> componentBetweenness(List<Node> component, double threshold, long seed, boolean directional){
		Map<String,Edge> result = new HashMap<String,Edge>();;
		//reset weights
		for (Edge current : edgeList.values()){
			current.setWeight(0);
		}
		//Dijkstra for all Nodes calcs betweenness
		if (threshold == Double.POSITIVE_INFINITY){
			for (Node current : component){
				ch.dijkstra(current,new HashMap<String,Node>());
				result = ch.getShortestEdges(component, true, result, directional,seed);
			}
		}else { //Dijkstra for as long as threshold is not overdone
			float highestBetweenness = 0;
			List<Node> subset = new ArrayList<Node>();
			Random rng = new Random(seed);
			while (highestBetweenness < threshold+component.size()-1 && subset.size()<component.size()){
				boolean isDrawn = false;
				Node drawn = null;
				long time = System.currentTimeMillis();
				while (!isDrawn){
					drawn = component.get(rng.nextInt(component.size()));
					if (!subset.contains(drawn)){ 
						subset.add(drawn);
						isDrawn=true;
					}
				}
				//System.out.println("Draw: "+(System.currentTimeMillis()-time)+" mills");
				time = System.currentTimeMillis();
				//Dijkstra can not be done only on the subset because that would exclude neighbors. but assign neighbors assures that that is not the case
				ch.dijkstra(drawn,new HashMap<String,Node>());
				//System.out.println("Dijkstra: "+(System.currentTimeMillis()-time)+" mills");
				time = System.currentTimeMillis();
				for (Node current:subset){
					result = ch.getShortestEdges(subset, true, result, directional,current,seed);
				}
				//System.out.println("Shortest Edges: "+(System.currentTimeMillis()-time)+" mills");
				time = System.currentTimeMillis();
				//get highest betweenness
				for (Edge intraEdge :result.values()){ 
					if (intraEdge.getWeight()>highestBetweenness) highestBetweenness = intraEdge.getWeight();
				}
				//System.out.println("Highest Betweenness: "+(System.currentTimeMillis()-time)+" mills");
				//System.out.println("Highest betweenness is "+highestBetweenness+ "\t between "+subset.size()+" nodes.");
			}
		}
		return result;
	}		
}