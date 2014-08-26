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

import data.Edges;
import data.Nodes;

/**
 * Class to read GDF Files
 * @author Marvin
 * 
 */
public class GDFReader {
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
	
	
	public static List<Edges> GetEdges(String file){
		List<Edges> edgeList = new ArrayList<Edges>();
		//read file
		StringBuilder edgeStrings = read(file);
		//find first line of edge 
		int firstEdge = 0;
		String[] lines = edgeStrings.toString().split(System.lineSeparator());
		for(int i = 0; i<lines.length ; i++){
			String s = lines[i];
			if (!s.equals("edgedef>node1 VARCHAR,node2 VARCHAR")) continue;
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
			Edges e = new Edges(source,target,weight);
			edgeList.add(e);
		}
		
		return edgeList;
	}
	
	public static List<Nodes> GetNodes(String file){
		List<Nodes> nodeList = new ArrayList<Nodes>();
		//read file
		StringBuilder edgeStrings = read(file);
		//find first line of edge 
		int lastNode = 0;
		String[] lines = edgeStrings.toString().split(System.lineSeparator());
		//get the last node
		for(int i = 0; i<lines.length ; i++){
			String s = lines[i];
			if (!s.equals("edgedef>node1 VARCHAR,node2 VARCHAR")) continue;
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
			Nodes n = new Nodes(id,label);
			nodeList.add(n);
		}
		
		return nodeList;
	}
	
	public static void GDFtoSQL(String file){
		//Used Variables
		List<Edges> edgeList = GetEdges(file);
		List<Nodes> nodeList = GetNodes(file);
		
		//Establish connections
		Connection connection = null;
		Statement statement = null;
		try {
			connection = JDBCMySQLConnection.getConnection();
			statement = connection.createStatement();
			
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
			Iterator<Nodes> itN = nodeList.iterator();
			while (itN.hasNext()){
				Nodes node = itN.next();
				String query = "INSERT INTO Nodes VALUES (" + node.getId() + ",'"+node.getLabel()+"');";
				try{
					statement.executeUpdate(query);	
				} catch (SQLException e){
					e.printStackTrace();
				}
			}
			
			//Write Edges in DB
			Iterator<Edges> itE = edgeList.iterator();
			while (itE.hasNext()){
				Edges edge = itE.next();
				String query = "INSERT INTO Edges VALUES (" + edge.getSource() + ","+edge.getTarget()+","+edge.getWeight()+");";
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
