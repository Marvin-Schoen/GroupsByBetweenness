package dataProcessing;

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

import data.Edge;
import data.Node;

public class ComponentHelper {
	public int edgesRemoved;
	Connection connection = null;
	Statement statement = null; 
	Random rng = null;
	String schema;
	
	/**
	 * Constructor
	 * @param nodeList List of Nodes in the Graph
	 * @param edgeList List of Edges in the Graph
	 * @param schema name of the database
	 */
	public ComponentHelper(String schema,long seed){
		this.edgesRemoved=0;
		this.schema=schema;
		connection = JDBCMySQLConnection.getConnection(schema);
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		rng = new Random(seed);
	}
	
	public void resetEdges(){
		String query = "UPDATE Edges SET deleted=0;";
		try {
			statement.executeUpdate(query);			
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * Writes the Nodes from the Sets to a File. Each Note has a value for how often it is contained in a certain community.
	 * @param sets the names of the tables that contain the communities
	 * @param path Save path of the file
	 */
	public void writeGroupsToFile(List<String> sets,String path){	
		try{
			//create table that holds how many times a node is in a community
			statement.executeUpdate("DROP TABLE IF EXISTS comtimes;");
			statement.executeUpdate("CREATE TABLE comtimes (nodeID double NOT NULL, communityID int NOT NULL, times int, PRIMARY KEY (nodeID, communityID));");
			
			// table from set is communitytableX(communityID INT, nodeID DOUBLE)
			for  (String set : sets){
				Statement nodeStatement = connection.createStatement();
				ResultSet node = nodeStatement.executeQuery("SELECT * FROM "+set);
				while (node.next()){
					//get number of times and increase
					int times = 0;
					Statement timesStatement = connection.createStatement();
					ResultSet comTimeNode = timesStatement.executeQuery("SELECT times FROM comtimes WHERE nodeID = "+node.getString("nodeID")+" AND communityID="+node.getString("communityID"));
					if (comTimeNode.next())
						times = comTimeNode.getInt("times");
					times++;
					statement.executeUpdate("INSERT INTO comtimes VALUES ("+node.getString("nodeID")+","+node.getString("communityID")+","+times+")"
							+ "ON DUPLICATE KEY UPDATE times = "+times+";");
				}
			}
		} catch (SQLException e){
			e.printStackTrace();
		}
		//Write list to Text File
		try{
			FileWriter write = new FileWriter(path,false);
			PrintWriter printLine = new PrintWriter(write);
			printLine.println("sep=\t");
			printLine.println("Name"+"\t"+"Community"+"\t"+"Times in Community");
			
			ResultSet node = statement.executeQuery("SELECT * FROM comtimes");
			while (node.next()){
				printLine.println(node.getString("nodeID")+"\t"+node.getString("communityID")+"\t"+node.getString("times"));
			}
		} catch (IOException e){
			e.printStackTrace();
		} catch (SQLException e){
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
			System.out.println("Wrote findings to file: "+path);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Removes both neighbor pointers from the source and target of the edge
	 * @param edge Edge 
	 */
	public void removeEdge(Edge edge){
		String query = "UPDATE edges SET deleted = 1 WHERE source="+edge.getSource()+" AND "+"target="+edge.getTarget()+";";
		try {
			statement.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		edgesRemoved++;
		System.out.print(edgesRemoved+":\t"+edge.getSource()+"\t\t-\t"+edge.getTarget());		
	}
	
	/**
	 * Returns edge from source to target or the other way around if undirectional
	 * @param source ID of the source Node
	 * @param target ID of the target Node
	 * @param direction if not direction edge will also be weigthed if it goes into the other direction
	 * @return
	 */
	public  void addEdgeWeight(String source, String target, boolean directional){
		
		String query = "SELECT * FROM edges WHERE source="+source+" AND target="+target+";";
		ResultSet rs = null;
		try {
			Statement rsStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE); 
			rs= rsStatement.executeQuery(query);
			if (!rs.next()){
				 if (!directional){
					query = "SELECT * FROM edges WHERE source="+target+" AND target="+source+";";
					try {
						rs= rsStatement.executeQuery(query);
						rs.next(); // if clause used next (even if rs has entries) so rs has to be set to 1 here
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				} else {
					System.out.println("Edge from "+source+" to "+target+" not found.");
					return ;
				}
			}
			//Add the weight
			float newWeight = rs.getFloat("weight")+0.5f; 
			rs.updateFloat("weight", newWeight);
			rs.updateRow();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	

	
	/*
	 * Adds a weight to an edge
	 * @param source ID of the source Node
	 * @param target ID of the target Node
	 * @param direction if not direction edge will also be weigthed if it goes into the other direction
	 * @return
	 *
	public  Edge addWeight(String source,String target, boolean directional){
		Edge edge = getEdge(source, target, directional);
		if (edge == null) System.out.println("Did you do a directional analysis on a nondirectional network? Null pointer exception in 3..2..1..");
		edge.setWeight(edge.getWeight()+0.5f); //0.5 because in the end it would be devided by two
		return edge;
	}*/
	
	
	/**
	 * Calculates for all other notes the shortest distance to the source node and sets their distance to it. Then sets
	 * the predecessing node to the current node which leads to the shortest path. Also adds weights to the notes
	 * according to how often they are part of a shortest path. ATTENTION: Do not forget to assign neighbors befor the first usage of Dijkstra
	 * @param sourceID sql ID of the Node to which all smallest distances should be calculated.
	 * @param directional if the network is directional
	 */
	public  void dijkstra(String sourceID, boolean directional){
		System.out.println("Starting Dijkstra");
		long start=System.currentTimeMillis();
		ResultSet rs = null;
		
		//Reset all Nodes, not only that in the list. distance -1 means currently not connected
		String query = "UPDATE nodes SET distance=-1, visited = 0 WHERE distance > -1 OR visited = 1;";
		try {statement.executeUpdate(query);
		} catch (SQLException e) {	e.printStackTrace();}
		
		query = "DELETE FROM previous;";
		try {statement.executeUpdate(query);
		} catch (SQLException e) {	e.printStackTrace();}
		
		
		//save values in source node
		Node source = null;
		try{
			rs=statement.executeQuery("SELECT * FROM nodes WHERE id="+sourceID);
			rs.next();
			source = new Node(rs.getString("id"), rs.getString("label"));
		} catch (SQLException e){
			e.printStackTrace();
		}
		
		
		//Distance for source is zero-> is the first one chosen
        source.setDistance(0.);
        try{
        	statement.executeUpdate("UPDATE nodes SET distance="+source.getDistance()+" WHERE id = "+source.getId()+";");
        } catch (SQLException e){
        	e.printStackTrace();
        }
        try{
	        //List of unvisited nodes	        
	        statement.executeUpdate("CREATE TABLE IF NOT EXISTS unvisited (nodeID bigint(20), distance int(11));");      
	        statement.executeUpdate("DELETE FROM unvisited");
	        Node current = source;        
	        statement.executeUpdate("INSERT INTO unvisited VALUES("+source.getId()+","+source.getDistance()+");");
	        
	        ResultSet unvisited = null;
	        System.out.println("needed "+(System.currentTimeMillis()-start)+" milliseconds to get to the first unvisited");
	        do {
	        	long beginUnv = System.currentTimeMillis();
	        	//Reset connection to save memory
	        	connection.close();
	        	connection = JDBCMySQLConnection.getConnection(schema);
				statement = connection.createStatement();
		        statement.setFetchSize(50000);
		        
        		//Get Node with the smallest distance
        		Statement currStatement = connection.createStatement();
        		currStatement.setFetchSize(50000);
        		ResultSet currSet = currStatement.executeQuery("SELECT * FROM unvisited WHERE distance = (SELECT MIN(distance) FROM unvisited)");
        		currSet.next();
	        	current = new Node(currSet.getString("nodeId"), "", currSet.getDouble("distance"));
	        	statement.executeUpdate("DELETE FROM unvisited WHERE nodeID = "+current.getId());
	        	
	        	//Add neighbors to current node
	        	//current.setNeighbors(getNeighbors(current, directional)); //needs to much memory
	        	Statement neighborStatement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
	        	neighborStatement.setFetchSize(50000);
	        	ResultSet neighbors = null;
	        	if (directional)
	        		neighbors = neighborStatement.executeQuery("SELECT nodes.id, nodes.label, nodes.distance, nodes.visited  FROM edges JOIN nodes ON (edges.target = nodes.id) WHERE edges.deleted = 0 AND edges.source = "+current.getId()+";");
	        	else
	        		neighbors = neighborStatement.executeQuery("SELECT nodes.id, nodes.label, nodes.distance, nodes.visited  FROM edges JOIN nodes "+
	        													"ON (edges.target = nodes.id) WHERE edges.deleted = 0 AND edges.source = "+current.getId()+""+
    															"UNION"+
    															"SELECT nodes.id, nodes.label, nodes.distance, nodes.visited  FROM edges JOIN nodes "+
	        													"ON (edges.source = nodes.id) WHERE edges.deleted = 0 AND edges.target = "+current.getId()+";");
	        	
	        	//Set visited for current neighbor
	        	statement.executeUpdate("UPDATE nodes SET visited = 1 WHERE id = "+current.getId()+";");
	        	
	        	
		        //Calculate new distances
	        	//if (list.contains(current)) //TODO the current node actually must not be contained in the list. The List is just the collection of start and
	        	while (neighbors.next()){
		        	String nid = neighbors.getString("id"); // debug
	        		if (current.getDistance() +1 <= neighbors.getInt("Distance") || neighbors.getInt("Distance")==-1 ){
	        			//Set new distance       			     			     				
	        			neighbors.updateInt("distance",(int)current.getDistance()+1);
	        			neighbors.updateRow();
	        			
	        			//Add neighbor as previous for current
	        			rs = statement.executeQuery("SELECT * FROM previous WHERE source="+neighbors.getString("id")+" AND target="+current.getId()+";");
	        			if (!rs.next())
	        				statement.executeUpdate("INSERT INTO previous VALUES ("+neighbors.getString("id")+","+current.getId()+");");

	        			boolean visited = neighbors.getBoolean("visited");
	        			if (!visited)        				
	        				statement.executeUpdate("INSERT INTO unvisited VALUES("+neighbors.getString("id")+","+neighbors.getString("distance")+");");
	        		}	        	
		        }
		        //update unvisited
		        Statement unvStatement = connection.createStatement();
		        unvStatement.setFetchSize(50000);
		        unvisited = unvStatement.executeQuery("SELECT * FROM unvisited");
		        System.out.println("needed "+(System.currentTimeMillis()-beginUnv)+" milliseconds to get to the next unvisited");
        	} while (unvisited.next());
        } catch (SQLException e){
    		e.printStackTrace();
    	}
	}
	
	/**
	 * returns a list of neighbors of the node
	 * @param node said Node
	 * @param directional if the network is directional
	 * @return list of Nodes
	 */
	public List<Node> getNeighbors(Node node, boolean directional){
		
		List<Node> result = new ArrayList<Node>();
		ResultSet rs = null;
		try{
			connection.close();
			connection = JDBCMySQLConnection.getConnection(schema);
			statement = connection.createStatement();
			//neighbors the node points at
			rs=statement.executeQuery("SELECT * FROM edges WHERE source="+node.getId()+" AND deleted = 0");
			if (rs.next())
				do {
					Statement StNode = connection.createStatement();
					ResultSet rsNode = StNode.executeQuery("SELECT * FROM nodes WHERE id="+rs.getDouble("target"));
					if (rsNode.next())
						result.add(new Node(rsNode.getString("id"),rsNode.getString("label"),rsNode.getInt("distance")));
				} while(rs.next());
			if (!directional){
				//neighbors that point at the node
				rs=statement.executeQuery("SELECT * FROM edges WHERE target="+node.getId()+" AND deleted = 0");
				while(rs.next()){
					Statement StNode = connection.createStatement();
					ResultSet rsNode = StNode.executeQuery("SELECT * FROM nodes WHERE id="+rs.getDouble("source"));
					if (rsNode.next())
						result.add(new Node(rsNode.getString("id"), rsNode.getString("label"),rsNode.getInt("distance")));
				}
			}
			
		} catch (SQLException e){
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * return the edges of the shortest paths of a List of Nodes. !!!Dijkstra must be done first and use the same list!!!!
	 * @param list name of sql table of Nodes for which the edges should be returned
	 * @param calcBetweenness if the betweenness for the edges should be calculated
	 * @param directional if the network is directional
	 */
	public void getShortestEdges(ResultSet list, boolean directional){
		try {
			//ResultSet list = statement.executeQuery("SELECT * FROM "+list+";");
			
			//go through the list of Nodes
			list.beforeFirst();
			while (list.next()){
				//Check if current node has previous
				Statement rsPrevStatement = connection.createStatement();
				String actNodeID = list.getString("nodeID");
				String query = "SELECT * FROM previous WHERE source="+actNodeID+";";
				ResultSet rsPrev = rsPrevStatement.executeQuery(query);
				if (!rsPrev.next())
					continue;
				
				do{ 
					//Randomly draw one of the predecessors
					rsPrev.last(); //point to last entry to get number of entries
					int n = rsPrev.getRow();			
					int prev = rng.nextInt(n)+1;
					rsPrev.beforeFirst();
					rsPrev.relative(prev);
					//add the weight
					addEdgeWeight(actNodeID,rsPrev.getString("target"), directional);
					//continue with predecessor
					actNodeID=rsPrev.getString("target");
					rsPrev = rsPrevStatement.executeQuery("SELECT * FROM previous WHERE source="+actNodeID+";");
				} while(rsPrev.next());
			}		
		}catch (SQLException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Return the edges of a shortest path of a node to the node for which the dijkstra was done before !!!Dont forget to do the dijkstra first!!
	 * @param list List of Nodes 
	 * @param directional if the network is directional
	 * @param from the source node
	 * @return
	 */
	public void getShortestEdges(ResultSet list, boolean directional, String from){
		try {
			//ResultSet list = statement.executeQuery("SELECT * FROM "+list+" WHERE id="+from+";");
			
			//go through the list of Nodes
			//list.next();
			//Check if current node has previous
			ResultSet rsPrev = statement.executeQuery("SELECT * FROM previous WHERE source="+from+";");
			if (!rsPrev.next()){
				//System.out.println("getShortestEdges could not find the from Node");
				return;
			}
			String actNodeID = from;
			do{ 
				//Randomly draw one of the predecessors
				rsPrev.last(); //point to last entry to get number of entries
				int n = rsPrev.getRow();			
				int prev = rng.nextInt(n)+1;
				rsPrev.beforeFirst();
				rsPrev.relative(prev);
				//add the weight
				addEdgeWeight(actNodeID,rsPrev.getString("target"), directional);
				//continue with predecessor
				actNodeID=rsPrev.getString("target");
				rsPrev = statement.executeQuery("SELECT * FROM previous WHERE source="+actNodeID+";");
			} while(rsPrev.next());
					
		}catch (SQLException e){
			e.printStackTrace();
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
	public int[] getNumberOfShortestPaths(String from,String to,String containing,boolean directional){		
		dijkstra(from,directional);
		int[] number = pathRecurator(to,containing,(containing=="")?true:false,directional);//If containing is null found is set to true because there is no specific node that should be in the path
		return number;
	}
	
	/**
	 * Recursive helper to calculate the number of shortests paths to a node
	 * @param node goal node
	 * @param containing node that should be contained in the path
	 * @param found if the node was in the path already
	 * @return number of shortest paths from this node to goal node. [0] contains the node [1] does not
	 */
	private int[] pathRecurator(String fromID, String containingID, boolean found, boolean directional){
		int[] number = {0,0};
		//Check if this node is the one searched for
		if (fromID == containingID)
			found=true;
		try{
			ResultSet neighbors;
			//go through all neighbors
			if (directional)
				neighbors = statement.executeQuery("SELECT * FROM edges WHERE source = " +fromID);
			else
				neighbors = statement.executeQuery("SELECT * FROM edges WHERE source = " +fromID+" OR target = "+fromID);;				
			while (neighbors.next()){
				ResultSet prev = statement.executeQuery("SELECT * FROM previous WHERE source = " +fromID);
				if (!prev.next()){ //reached the end
					if (found) 
						return new int[] {1,0};
					else 
						return new int[] {0,1};
				} else {
					int[] intermediate = pathRecurator(prev.getString("target"), containingID, found, directional);
					number[0] += intermediate[0]; 
					number[1] += intermediate[1];
				}
			}
		}catch(SQLException e){
			e.printStackTrace();
		}			
		return number;
	}
}
