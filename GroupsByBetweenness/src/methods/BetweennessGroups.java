package methods;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import com.mysql.jdbc.ResultSetMetaData;

import data.Edge;
import data.Node;
import dataProcessing.GDFReader;
import dataProcessing.JDBCMySQLConnection;
import dataProcessing.SQLGrabber;
import dataProcessing.ComponentHelper;

public class BetweennessGroups {
	//private List<Node> nodeList;
	private List<Edge> edgeList;
	private ComponentHelper ch;
	Connection connection = null;
	Statement statement = null;
	Random rng = null;
	
	/**
	 * Constructur
	 * @param nodeList List of Nodes of the graph
	 * @param edgeList List of Edges of the graph
	 */
	public BetweennessGroups(String schema,long seed){
		this.ch = new ComponentHelper(schema,seed);

		connection = JDBCMySQLConnection.getConnection(schema);
		rng = new Random(seed);
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Repeatedly uses findBetwCommunities to get different sets of communities. Then names matching communities
	 * with the same name
	 * @param numberOfSets Number Of Sets to produce
	 * @param threshold Threshold for the betweenness for the adjusted brandes
	 * @return set of communities
	 */
	public  List<String> tyler(int numberOfSets,double threshold,long seed,boolean directional, boolean cont){
 		List<String> sets = new ArrayList<String>();
 		
 		//Delete communitysets from former stuff
 		try{
 			ResultSet rs = statement.executeQuery("SELECT CONCAT( 'DROP TABLE ', GROUP_CONCAT(table_name) , ';' ) AS statement FROM information_schema.tables WHERE table_schema = 'friendnet' AND table_name LIKE 'communities_%';");
 			rs.next();
 			String dropQueue = rs.getString(1);
 			if (dropQueue!=null)
 				statement.executeUpdate(dropQueue);
 		}catch (SQLException e){ 			
 			e.printStackTrace();
 		}
 		//Iterate for number of Sets
		for (int i =0;i<numberOfSets;i++){
			sets.add(findBetwCommunities(threshold,seed+i,directional,cont));
			System.out.println("Iteration "+(i+1));
		}
		try{
			//Give matching communities the same name
 			for (int i = 0;i<numberOfSets-1;i++){ // iterate through all sets
				//Strings to make the code more readable
				String currentTable = sets.get(i);
				String nextTable = sets.get(i+1);
				
				//This set and following set
				Statement currentStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet current = currentStatement.executeQuery("SELECT * FROM "+currentTable+";");
				Statement nextStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet next = nextStatement.executeQuery("SELECT * FROM "+nextTable+";");
				
				//Communities of the sets
				Statement currentComsStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet currentComs = currentComsStatement.executeQuery("SELECT communityID FROM "+currentTable+" GROUP BY communityID;");
				Statement nextComsStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet nextComs = nextComsStatement.executeQuery("SELECT communityID FROM "+nextTable+" GROUP BY communityID;");
				
				//number of communities
				currentComs.last();
				nextComs.last();
				int numberCurrentComs = currentComs.getRow();
				int numberNextComs = nextComs.getRow();
				currentComs.beforeFirst();
				nextComs.beforeFirst();
				
				//Communities in the current set
				for (int j = 0;j<numberCurrentComs;j++){
					currentComs.next();
					String currCommName = currentComs.getString("communityID");
					for (int k = 0; k<numberNextComs;k++){
						nextComs.next();
						String nextCommName = nextComs.getString("communityID");
						//number of identical nodes in communities j and k
						Statement sharedStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
						ResultSet shared = sharedStatement.executeQuery("SELECT * FROM "+currentTable+" INNER JOIN "+nextTable+" ON "
											+currentTable+".nodeID = "+nextTable+".nodeID WHERE "+
											currentTable+".communityID = "+currCommName+" AND "
												+nextTable+".communityID = "+nextCommName+";");
						shared.last();
						int sharedSize = shared.getRow();
						
						//number of entries in the communities
						ResultSet currentComSize = statement.executeQuery("SELECT COUNT(*) FROM "+ currentTable +" WHERE communityID = "+currCommName+";");
						currentComSize.next();
						int currentSize = currentComSize.getInt(1);
						ResultSet nextComSize = statement.executeQuery("SELECT COUNT(*) FROM "+ nextTable +" WHERE communityID = "+nextCommName+";");
						nextComSize.next();
						int nextSize = nextComSize.getInt(1);
						
						//if more than half of the entries are the same, the communities are the same
						boolean same = false; //if they are the same sets
						if (currentSize>nextSize){ // if first set is larget that second set
							if (sharedSize>=Math.ceil(currentSize/2.)) same = true;
						} else {
							if (sharedSize>=Math.ceil(nextSize/2.)) same = true;
						}
						
						//When they are the same set rename second set to the first set 
						if (same){
							//Set name of the community that blocks the right name to -1
							statement.executeUpdate("UPDATE "+nextTable+" SET communityID = -1 WHERE communityID = "+currCommName+";");
							//Set name of the next community to the current communities name
							statement.executeUpdate("UPDATE "+nextTable+" SET communityID = "+currCommName+" WHERE communityID = "+nextCommName+";");
							//Set the name of the community with -1 to the former name of the next community
							statement.executeUpdate("UPDATE "+nextTable+" SET communityID = "+nextCommName+" WHERE communityID = -1;");
													
							//Break inner for loop as similar group is found
							break;
						}
					}
					
				}
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
		return sets;
		///////////////////////////////////////////////////////////////////////////////
		//Give matching communities the same name
		/*List<Map<String,List<Node>>> sets = new ArrayList<Map<String,List<Node>>>();
		for (int i =0;i<numberOfSets;i++){
			//nodeList=ch.assignNeighbors(directional);	
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
							//Put nextEntry in right space
							nextSet.put(tmpString, nextEntry.getValue());
							//save old position of nextEntry
							tmpString = nextEntry.getKey();
							//put saved entry onto old position from nextentry												
							nextSet.put(tmpString, tmpList);
						}
						//Break inner for loop as similar group is found
						break;
					}					
				}
				
			}
		}
		return sets;*/
	}
	
	/**
	 * Finds communities based on the algorithm of Typer and all
	 * @param threshold Value for the dijkstra of how much the highest betweenness must be OVER the component size -1 (community criterion) to terminate
	 */
	public  String findBetwCommunities(double threshold, long seed, boolean directional, boolean cont){
		
		//"Break the graph into connected components"
		//-> Check for compontents that are not connected
		//Start with a Node Check the shortest distances to that node
		//If there are Nodes with infinite they are in another community
		//reduce the list to the ones with infinite distance
		//repeat that for the rest until there are no others
		
		//Bool determines if there are still nodes without groups
		boolean nodesLeft = true;
		String communityTable = "communities";
		try{
			if (!cont){ //if you don't continue start new
				//Initialize list to that has to get split. Is in SQL cause it may take all nodes (which can be 3GB)
				statement.executeUpdate("DROP TABLE IF EXISTS tosplit");
				statement.executeUpdate("CREATE TABLE tosplit (id DOUBLE);");
				statement.executeUpdate("INSERT INTO tosplit SELECT id FROM nodes;");
			
				//Initialize component list that has components of connected nodes
				statement.executeUpdate("DROP TABLE IF EXISTS components");
				statement.executeUpdate("CREATE TABLE components (componentID INT, nodeID DOUBLE);");			
				
				//Create continue table that save variables (This is also good for monitoring)
				statement.executeUpdate("DROP TABLE IF EXISTS cont");
				statement.executeUpdate("CREATE TABLE cont (setName INT, i INT, n INT);");
			}
			int componentNumber = 1;
			ResultSet rs = null;			
			rs = statement.executeQuery("SELECT MAX(componentID) FROM components");
			if (rs.next())
				componentNumber = rs.getInt(1)+1;
			while (nodesLeft){
				//1. Get all Nodes connected to node 0
				rs = statement.executeQuery("SELECT id FROM tosplit LIMIT 1");
				if (rs.next())
					ch.dijkstra(rs.getString("id"),directional);
				
				//sort out connected and unconnected nodes. Unconnected nodes have distance -1
				///List with connected nodes
				statement.executeUpdate("DROP TABLE IF EXISTS connected");
				statement.executeUpdate("CREATE TABLE connected (SELECT id FROM nodes distance != -1);");
				///List with Unconnected nodes
				statement.executeUpdate("DROP TABLE IF EXISTS unconnected");
				statement.executeUpdate("CREATE TABLE unconnected (SELECT nodes.id FROM tosplit INNER JOIN nodes ON tosplit.id = nodes.id WHERE nodes.distance = -1);");
				
				//connected Parts form a new component
				rs= statement.executeQuery("SELECT * FROM connected");
				if (rs.next()){
					statement.executeUpdate("INSERT INTO components SELECT '"+componentNumber+"' AS componentID , id FROM connected;");
					componentNumber++;
				}
				
				//Check if there are still unconnected components
				rs= statement.executeQuery("SELECT * FROM unconnected");
				if (!rs.next())
					nodesLeft = false;
				else {
					statement.executeUpdate("DELETE FROM tosplit");
					statement.executeUpdate("INSERT INTO tosplit (SELECT * FROM unconnected)");
				}
					
			}
						
			//Create Community set
			int comNum = 1; //community Number
			communityTable = "communities" + comNum;
			boolean created =false;
			if (cont){ //if continue say on which table				
				rs= statement.executeQuery("SELECT community FROM cont");
				if (rs.next()){
					comNum = rs.getInt(1);
					communityTable = "communities" + comNum;
					created = true;
				} 
			}
			while (!created){
				try{
					statement.executeUpdate("CREATE TABLE "+communityTable+" (communityID INT, nodeID DOUBLE);");
					created = true;
				}catch (SQLException e){
					comNum++;
					communityTable = "communities" + comNum;
				}
			}			
			rs= statement.executeQuery("SELECT community FROM cont"); //Check if there is an entry
			if (!rs.next())
				statement.executeUpdate("INSERT INTO cont VALUES (0,0,0);");
			statement.executeUpdate("UPDATE cont SET setName = "+communityTable); //Save for later
			
			
			//"For each component, check to see if component is a community."
			///Set community Number (If continue take the highest)
			int communityNumber = 1;
			rs = statement.executeQuery("SELECT MAX(communityID) FROM "+communityTable);
			if (rs.next())
				communityNumber = rs.getInt(1)+1;
			
			int n = 0;
			int i=1;
			if (cont){
				rs = statement.executeQuery("SELECT i,n FROM cont");
				rs.next();
				i = rs.getInt("i");
				n = rs.getInt("n");		
			} 			
			if (n==0){ //this happens if former runs of the program did not come this far or !cont
				rs = statement.executeQuery("SELECT COUNT(DISTINCT componentID) as n FROM components;");
				rs.next();
				n = rs.getInt("n"); //number of components	
			}			
			for (; i<=n;i++){
				// a component of five or less vertices can not be further divided
				Statement compStatement = connection.createStatement();
				ResultSet component = compStatement.executeQuery("SELECT * FROM components WHERE componentID = "+i+";");
				component.last();
				int compSize = component.getRow();
				component.beforeFirst();
				if (compSize<6){ //less than 6 vertices in component
					statement.executeUpdate("INSERT INTO "+communityTable+" SELECT '"+communityNumber+"' AS communityID,nodeID FROM components WHERE componentID="+i+";");
					statement.executeUpdate("DELETE FROM components WHERE componentID="+i+";");
					communityNumber++;
					//i=i-1; //index is lowered because of removal
					//n=n-1; //number of components is lowered because of the deleted component
				}else{ // a component with n vertices has the highest betweenness of n-1 it is a community
					//Calculate highest betweenness
					componentBetweenness(component,threshold,directional);
					Statement rsHBStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
					ResultSet rsHB= rsHBStatement.executeQuery("SELECT MAX(weight) AS weight FROM edges;");
					rsHB.next();
					float highestBetweenness = rsHB.getFloat("weight");
					rsHB= rsHBStatement.executeQuery("SELECT * FROM edges WHERE weight = "+highestBetweenness+";");
					if (highestBetweenness<=compSize-1){
						//new community found through leaf criterion
						statement.executeUpdate("INSERT INTO "+communityTable+" SELECT '"+communityNumber+"' AS communityID, nodeID FROM components WHERE componentID = "+i+";");
						communityNumber++;
						statement.executeUpdate("DELETE FROM components WHERE componentId = "+i+";");
						//i=i-1; //index is lowered because of removal
						//n=n-1; //number of components is lowered because of the deleted component
					}else{ //Component is no community. Remove edges until graph is split in two components
						//find edges with highest betweenness (already in rsHB) and delete one random
						rsHB.last();
						int rshbSize = rsHB.getRow();
						int rnd = rng.nextInt(rshbSize)+1;
						rsHB.beforeFirst();
						rsHB.relative(rnd);
						rsHB.updateInt("deleted", 1);
						rsHB.updateRow();
						
						//Check if the edge removal has created two components
						component.first();
						ch.dijkstra(component.getString("nodeId"),directional);
						
						//sort out connected and unconnected nodes. Unconnected nodes have distance -1
						///List with connected nodes
						statement.executeUpdate("DROP TABLE connected");
						statement.executeUpdate("CREATE TABLE connected (SELECT id FROM components INNER JOIN nodes ON components.nodeId = nodes.id WHERE nodes.distance!=-1 AND components.componentID="+i+");");
						///List with Unconnected nodes
						statement.executeUpdate("DROP TABLE unconnected");
						statement.executeUpdate("CREATE TABLE unconnected (SELECT id FROM components INNER JOIN nodes ON components.nodeId = nodes.id WHERE nodes.distance=-1 AND components.componentID="+i+");");
						
						//unconnected Nodes
						Statement rsStatement = connection.createStatement();
						rs= rsStatement.executeQuery("SELECT * FROM unconnected");
						if (!rs.next())
							i=i-1;//Alle schritte füe gleiche Komponente noch ein mal durch gehen.
						else{// die beiden aufgespalteten Componentenar weiter untersuchen
							statement.executeUpdate("DELETE FROM components WHERE componentId="+i+";");
							statement.executeUpdate("INSERT INTO components SELECT '"+i+"' AS componentID, id AS nodeID FROM connected;");
							i=i-1; //index ist lowered because of removal
							n=n+1; // one removed two added
							statement.executeUpdate("INSERT INTO components SELECT '"+n+"' AS componentID, id AS nodeID FROM unconnected;");							
						}
					}
					
				}
				statement.executeUpdate("UPDATE cont SET i="+i+",n="+n);
			}
		} catch (SQLException e){
			e.printStackTrace();
		}
		
		return communityTable;
		///////////////////////////////////////////////////////////////////////////////////////////
		
		//"For each component, check to see if component is a community."
		/*Map<String,List<Node>> communities = new HashMap<String,List<Node>>(); // List of finished communities
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
					ch.removeEdge(toDelete);
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

		return communities;*/
	}
	
	/**
	 * Calculate betweenness for whole component. Resets all edge weights beforehand so the highest betweenness is definetly from this component
	 * @param component name of the table that contains the List of Nodes
	 * @param threshold Value of how much the highest betweenness must be OVER the component size -1 (community criterion) to terminate
	 */
	public void componentBetweenness(ResultSet component, double threshold, boolean directional){
		try {
			//reset weights
			Statement cbStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
			cbStatement.executeUpdate("UPDATE edges SET weight = 0;");
			
			//Dijkstra for all Nodes
			if (threshold == Double.POSITIVE_INFINITY){
				while (component.next()){
					ch.dijkstra(component.getString("nodeID"), directional);
					int row = component.getRow(); //Save row to continue later
					ch.getShortestEdges(component, directional);				
					component.beforeFirst();
					component.relative(row);
				}
			} else { //Dijkstra for as long as threshold is not overdone
				float highestBetweenness = 0;
				cbStatement.executeUpdate("DROP TABLE IF EXISTS subset;");
				cbStatement.executeUpdate("CREATE TABLE subset (id DOUBLE, PRIMARY KEY (id));");
				Statement rsSubStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet rsSub = rsSubStatement.executeQuery("SELECT * FROM subset;");
				
				//get component size
				component.last();
				int compSize = component.getRow();
				component.beforeFirst();
				
				int subSize = 0;
				while (highestBetweenness < threshold+compSize-1 && subSize<compSize){ //divide by two because weight is for both ways
					//draw node from component ins subset
					boolean isDrawn = false;
					String drawn = "";
					while (!isDrawn){
						int r = rng.nextInt(compSize)+1;
						component.beforeFirst();
						component.relative(r);
						drawn = component.getString("nodeID");
						Statement contStatement = connection.createStatement();
						ResultSet contains = contStatement.executeQuery("SELECT * FROM subset WHERE id="+drawn+";");
						if (!contains.next())
							isDrawn=true;
					}
					//Insert drawn into table
					rsSub.moveToInsertRow();
					rsSub.updateDouble("id", Double.parseDouble(drawn));
					rsSub.insertRow();
					rsSub.moveToCurrentRow();
					rsSub.beforeFirst();
					subSize++;
					
					//dijkstra to the drawn node
					ch.dijkstra(drawn, directional);
					//calculate betweenness by calculating paths from all nodes of the subset to the newly drawn node
					while (rsSub.next()){
						ch.getShortestEdges(rsSub, directional, rsSub.getString(1));
					}
					
					//get highest betweenness
					Statement rsHBStatement = connection.createStatement();
					ResultSet rsHB= rsHBStatement.executeQuery("SELECT MAX(weight) AS hb FROM edges;");
					if (rsHB.next())
						highestBetweenness = rsHB.getFloat("hb");
				}
			}	
		}catch (SQLException e){
			e.printStackTrace();
		}
	}		
}
