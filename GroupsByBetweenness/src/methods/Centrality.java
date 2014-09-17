package methods;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import data.Edge;
import data.Node;
import dataProcessing.ComponentHelper;
import dataProcessing.JDBCMySQLConnection;

public class Centrality {
	private ComponentHelper ch;
	Connection connection = null;
	Statement statement = null;
	
	public Centrality(String schema, long seed){
		ch = new ComponentHelper(schema,seed);
		
		connection = JDBCMySQLConnection.getConnection(schema);
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Calculates the degree centrality of the node. "Directed" is determined by assign neighbors
	 * @param nodeID id of the node in the table
	 * @param directional if the network is relational
	 * @return degree Centrality
	 */
	public float degreeCentrality(String nodeID,boolean directional){
		float centrality = 0;
		Node node = new Node(nodeID,"");
		//go through all edges and see if it is ingoing or outgoing for the node
		centrality+=ch.getNeighbors(node,directional).size();
		//centrality is normalized by n-1
		int n = 0;
		try {
			n = statement.executeQuery("SELECT COUNT(*) FROM nodes").getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return centrality/(n-1);
	}
	
	/**
	 * Calculates the closeness centrality of the node. Dont forget to assign neigbors beforehand. Directional is determined by assign neighbors
	 * @param node named Node
	 * @return closeness centrality
	 */
	public float closenessCentrality (String nodeID,boolean directional){
		ch.dijkstra(nodeID, directional);
		float centrality = 0;
		int numReachableNodes = 0;
		try{
			//get the shortest path for all nodes to the source node
			ResultSet spl = statement.executeQuery("SELECT SUM (distance) FROM nodes;");
			centrality = spl.getFloat(1);
			
			//get number of reachable nodes from the node
			spl = statement.executeQuery("SELECT COUNT (*) FROM nodes WHERE distance=-1");
			numReachableNodes = spl.getInt(1);
		}catch (SQLException e){
			e.printStackTrace();
		}
		
		int n = 0;
		try {
			n = statement.executeQuery("SELECT COUNT(*) FROM nodes").getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//normalized by the number of reachable nodes and the number of nodes in the network
		float a = (float) numReachableNodes / (float) (n-1) ;
		float b = (float) centrality / (float) numReachableNodes;
		float c = a / b;
		return c ;
		
		/////////////////////////////////////
		/*float centrality = 0;
		ch.dijkstra(nodeID, directional);
		//get the shortest path for all nodes to the source node
		for (Node current:nodeList){
			if (current.getDistance()<Double.POSITIVE_INFINITY)
				centrality+=current.getDistance();
		}
		//get number of reachable nodes from the node
		int numReachableNodes = 0;
		for (Node current : nodeList){
			if (current.getPrevious()!=null)
				numReachableNodes++;
		}
		
		//normalized by the number of reachable nodes and the number of nodes in the network
		float a = (float) numReachableNodes / (float) (nodeList.size()-1) ;
		float b = (float) centrality / (float) numReachableNodes;
		float c = a / b;
		return c ;*/
	}
	
	/**
	 * Calculates the betweenness centrality of a node
	 * @param node Node for which the centrality should be calculated
	 * @param directional If the network has directional relations
	 * @return betweenness centrality
	 */
	public float betweennessCentrality (String nodeID,boolean directional){
		float sum = 0; //sum of shortests paths containing the node divided by the sum of shortest paths
		//get list of connected Nodes
		ch.dijkstra(nodeID, directional);		
		int connectedSize = 0;
		try{
			ResultSet connected = statement.executeQuery("SELECT * FROM nodes WHERE distance != -1");
			
			//Get connected Size
			connected.last();
			connectedSize = connected.getRow();
			connected.beforeFirst();
			
			//loop to calculate the sum
			for (int j = 1;j<connectedSize;j++){
				//get jNode
				connected.beforeFirst();
				connected.relative(j);
				String jNode = connected.getString("id");
				for (int k = j+1;k<connectedSize;k++){
					//get kNode
					connected.beforeFirst();
					connected.relative(k);
					String kNode = connected.getString("id");
					
					//calc shortest paths
					int shortestPaths[] = ch.getNumberOfShortestPaths(jNode, kNode, nodeID, directional);
					sum += (float) shortestPaths[0]/ (float) shortestPaths[1];
				}
			}
		} catch (SQLException e){
			e.printStackTrace();
		}
		return (float) sum/((float)(connectedSize-1)*(float)(connectedSize-2)/(float)2);
	}

}
