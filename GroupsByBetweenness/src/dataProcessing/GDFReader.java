package dataProcessing;

import dataProcessing.JDBCMySQLConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import data.Edge;
import data.Node;

/**
 * Class to read GDF Files
 * @author Marvin
 * 
 */
public class GDFReader {
	/**
	 * Main method. Reads a GDF file. Path has to be edited in codem or through params.
	 * @param pommes arguments normaly called args. [0]= filename ; [1]= schema name
	 */
	public static void main(String pommes[]){
		String path = "C:\\Users\\Marvin\\Desktop 2\\groupinteractions_fbiphone.gdf";
		String schema = "fbiphone";
		if (pommes.length==2){
			path="C:\\Users\\Marvin\\Desktop 2\\"+pommes[0];
			schema=pommes[1];
		}
		GDFtoSQL(path,schema);
	}
	
	/**
	 * Reads the file and outputs a StringBuilder
	 * @param file Location of the file in string form
	 * @return StringBuilder
	 */
	public static StringBuilder read(String file){
		StringBuilder sb = new StringBuilder();
		
		BufferedReader br = null;
	    try{
	    	br = new BufferedReader(new FileReader(file));
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            sb.append(System.lineSeparator());
	            line = br.readLine();
	        }
	    } catch (Exception e){
	    	e.printStackTrace();
	    } finally {
	    	if (br != null){
		        try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	}
	    }		
		
		return sb;		
	}
	
	
	public static List<Edge> GetEdges(String file){
		List<Edge> edgeList = new ArrayList<Edge>();
		//read file
		StringBuilder edgeStrings = read(file);
		//find first line of edge 
		int firstEdge = 0;
		String[] lines = edgeStrings.toString().split(System.lineSeparator());
		for(int i = 0; i<lines.length ; i++){
			String s = lines[i];
			if (!s.startsWith("edgedef>node1 VARCHAR,node2 VARCHAR")) continue;
			else {
				firstEdge = i+1;
				break;
			}
		}
		if (firstEdge == 0) return null;
		
		//Make objects from String edges
		for(int i = firstEdge ; i<lines.length;i++){
			String[] actors = lines[i].split(",");
			String source = actors[0];
			String target = actors[1];
			int weight = 0;
			Edge e = new Edge(source,target,weight);
			edgeList.add(e);
		}
		
		return edgeList;
	}
	
	public static List<Node> GetNodes(String file){
		List<Node> nodeList = new ArrayList<Node>();
		//read file
		StringBuilder edgeStrings = read(file);
		//find first line of edge 
		int lastNode = 0;
		String[] lines = edgeStrings.toString().split(System.lineSeparator());
		//get the last node
		for(int i = 0; i<lines.length ; i++){
			String s = lines[i];
			if (!s.startsWith("edgedef>node1 VARCHAR,node2 VARCHAR")) continue;
			else {
				lastNode = i-1;
				break;
			}
		}
		if (lastNode == 0) return null;
		
		//Make objects from String edges
		for(int i = 1 ; i<=lastNode;i++){
			String[] attributes = lines[i].split(",");
			String id = attributes[0];
			String label = attributes[1];
			Node n = new Node(id,label);
			nodeList.add(n);
		}
		
		return nodeList;
	}
	
	public static void GDFtoSQL(String file, String schema){
		//Used Variables
		List<Edge> edgeList = GetEdges(file);
		List<Node> nodeList = GetNodes(file);
		
		//Establish connections
		Connection connection = null;
		Statement statement = null;
		try {
			connection = JDBCMySQLConnection.getConnection("");
			statement = connection.createStatement();			
			try{
				statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS "+schema);
				connection.close();
			} catch (SQLException e){
				e.printStackTrace();
			}
			connection = JDBCMySQLConnection.getConnection(schema);
			statement = connection.createStatement();
			try{
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS `nodes` "
						+ "(  `id` varchar(45) NOT NULL,  "
						+ "`label` varchar(45) DEFAULT NULL,  "
						+ "`distance` int(11) DEFAULT NULL,  "
						+ "PRIMARY KEY (`id`)) "
						+ "ENGINE=InnoDB DEFAULT CHARSET=utf8;");
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS `edges` ("
						+ "  `source` varchar(45) NOT NULL,"
						+ "  `target` varchar(45) NOT NULL,"
						+ "  `weight` float DEFAULT NULL,"
						+ "  `deleted` tinyint(1) DEFAULT NULL,"
						+ "  PRIMARY KEY (`source`,`target`),"
						+ "  KEY `target_idx` (`target`),"
						+ "  CONSTRAINT `source` FOREIGN KEY (`source`) REFERENCES `nodes` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,"
						+ "  CONSTRAINT `target` FOREIGN KEY (`target`) REFERENCES `nodes` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION"
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
			} catch (SQLException e){
				e.printStackTrace();
			}
			//Clear Table
			String deleteEdges = "DELETE FROM Edges";
			String deleteNodes = "DELETE FROM Nodes";	
			try{
				statement.executeUpdate(deleteEdges);
				statement.executeUpdate(deleteNodes);				
			} catch (SQLException e){
				e.printStackTrace();
			}
			//Write Nodes in DB
			Iterator<Node> itN = nodeList.iterator();
			while (itN.hasNext()){
				Node node = itN.next();
				String query = "INSERT INTO Nodes VALUES ('" + node.getId() + "','"+node.getLabel()+"',-1);";;
				try{
					statement.executeUpdate(query);	
				} catch (SQLException e){
					e.printStackTrace();
				}
			}
			
			//Write Edges in DB
			Iterator<Edge> itE = edgeList.iterator();
			while (itE.hasNext()){
				Edge edge = itE.next();
				String query = "INSERT INTO Edges VALUES ('" + edge.getSource() + "','"+edge.getTarget()+"','"+edge.getWeight()+"',0);";
				try{
					statement.executeUpdate(query);		
				} catch (SQLException e){
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
