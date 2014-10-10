package dataProcessing;

import dataProcessing.JDBCMySQLConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import data.Edge;
import data.Node;

public class SQLGrabber {
	
	public static List<Node> grabNodes(String schema){
		//Empty Variables
		List<Node> nodeList = new ArrayList<Node>();
		ResultSet rs = null;
		Connection connection = null;
		Statement statement = null; 
		String query = "SELECT * FROM Nodes";
		try {			
			connection = JDBCMySQLConnection.getConnection(schema);
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			
			while(rs.next()){
				nodeList.add(new Node(rs.getString("id"),rs.getString("label")));
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
		return nodeList;
	}
	
	public static List<Edge> grabEdges(String schema){
		//Empty Variables
		List<Edge> edgeList = new ArrayList<Edge>();
		ResultSet rs = null;
		Connection connection = null;
		Statement statement = null; 
		String query = "SELECT * FROM Edges";
		try {			
			connection = JDBCMySQLConnection.getConnection(schema);
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			
			while(rs.next()){
				edgeList.add(new Edge(rs.getString("source"),rs.getString("target"),1/*rs.getInt("weight")*/));
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
		return edgeList;
	}
	
	public static void saveSets(List<Map<String,List<Node>>> sets,boolean directed){
		//Establish connections
		Connection connection = null;
		Statement statement = null;		
		int schemaNumber = 0;
		try {
			connection = JDBCMySQLConnection.getConnection("");
			statement = connection.createStatement();
			//Create new Schemaint
			
			boolean schemaCreated = false;
			while (!schemaCreated){
				schemaCreated = true;
				String schemaQuery = "CREATE SCHEMA `communitysets"+schemaNumber+"` ;";
				try{statement.executeUpdate(schemaQuery);	} catch (SQLException e){
					schemaCreated = false;
					schemaNumber++;
				}
			}
			//Tables
			for (int i = 0;i<sets.size();i++){
				//Variables
				List<Edge> edgeList = new ArrayList<Edge>();
				//Create Tables for Set i
				String nodeQuery = 	"CREATE TABLE `communitysets"+schemaNumber+"`.`nodes"+i+"` ( "+
									"`id` DOUBLE NOT NULL, "+
									"`label` VARCHAR(45) NULL, "+
									"`community` VARCHAR(45) NULL, "+
									"PRIMARY KEY (`id`) "
									+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
				
				String edgeQuery = 	"CREATE TABLE `communitysets"+schemaNumber+"`.`edges"+i+"` ( "+
						  "`source` double NOT NULL, "+
						  "`target` double NOT NULL, "+
						  "`weight` int(11) DEFAULT NULL, "+
						  "PRIMARY KEY (`source`,`target`), "+
						  "KEY `target_idx` (`target`), "+
						  "CONSTRAINT `source"+i+"` FOREIGN KEY (`source`) REFERENCES `nodes"+i+"` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, "+
						  "CONSTRAINT `target"+i+"` FOREIGN KEY (`target`) REFERENCES `nodes"+i+"` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION "+
						  ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ";
				try{statement.executeUpdate(nodeQuery); } catch (SQLException e){e.printStackTrace();}
				try{statement.executeUpdate(edgeQuery);	} catch (SQLException e){
					e.printStackTrace();}			
				
					
				
				nodeQuery = "";
				edgeQuery = "";
				
				Map<String,List<Node>> set = sets.get(i);
				//Communities
				for (Map.Entry<String, List<Node>> entry : set.entrySet()){
					List<Node> community = entry.getValue();
					if (community == null)continue;
					//Community Name
					String communityName = entry.getKey();
					//Nodes
					for (Node node : community){
						//Create Edges from Node
						for (Node neighbor : node.getNeighbors()){
							Edge edge = new Edge(node.getId(),neighbor.getId(),1);
							//check if edge is already there
							boolean existing = false;
							for (Edge fromList : edgeList){
								if ((fromList.getSource()==edge.getSource() && fromList.getTarget() == edge.getTarget())
										|| (!directed && fromList.getSource()==edge.getTarget() && fromList.getTarget() == edge.getSource())){
									existing = true;
								}
							}
							if (!existing) edgeList.add(edge);							
						}
						//Add Node to Query
						nodeQuery="INSERT INTO `communitysets"+schemaNumber+"`.`nodes"+i+"` (`id`, `label`,`community`) VALUES ('"+node.getId()+"', '"+node.getLabel()+"', '"+communityName+"');";
						try{statement.executeUpdate(nodeQuery); } catch (SQLException e){e.printStackTrace();}
					}
				}
				//Add Edges to SQL

				for (Edge edge : edgeList){
					edgeQuery = " INSERT INTO `communitysets"+schemaNumber+"`.`edges"+i+"` (`source`, `target`, `weight`) "
							+ "VALUES ('"+edge.getSource()+"', '"+edge.getTarget()+"', '"+edge.getWeight()+"');";
					try{statement.executeUpdate(edgeQuery);} catch (SQLException e){e.printStackTrace();}
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
		System.out.println("Saved Nodes and Edges into database: communitysets"+schemaNumber);
	}
	
	/* 50 repetitions would produce 150gb of data this method is not applicable for huge sets
	public static void saveSets(List<String> sets,boolean directed, String schema){
		//Establish connections
		Connection writeConnection = null;
		Statement writeStatement = null;		
		int schemaNumber = 0;
		try {
			writeConnection = JDBCMySQLConnection.getConnection("");
			writeStatement = writeConnection.createStatement();
			//Create new Schemaint
			
			boolean schemaCreated = false;
			while (!schemaCreated){
				schemaCreated = true;
				String schemaQuery = "CREATE SCHEMA `communitysets"+schemaNumber+"` ;";
				try{writeStatement.executeUpdate(schemaQuery);	} catch (SQLException e){
					schemaCreated = false;
					schemaNumber++;
				}
			}
			//Tables
			for (int i = 0;i<sets.size();i++){
				//Variables
				List<Edge> edgeList = new ArrayList<Edge>();
				//Create Tables for Set i
				String nodeQuery = 	"CREATE TABLE `communitysets"+schemaNumber+"`.`nodes"+i+"` ( "+
									"`id` DOUBLE NOT NULL, "+
									"`label` VARCHAR(45) NULL, "+
									"`community` VARCHAR(45) NULL, "+
									"PRIMARY KEY (`id`) "
									+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
				
				String edgeQuery = 	"CREATE TABLE `communitysets"+schemaNumber+"`.`edges"+i+"` ( "+
						  "`source` double NOT NULL, "+
						  "`target` double NOT NULL, "+
						  "`weight` int(11) DEFAULT NULL, "+
						  "PRIMARY KEY (`source`,`target`), "+
						  "KEY `target_idx` (`target`), "+
						  "CONSTRAINT `source"+i+"` FOREIGN KEY (`source`) REFERENCES `nodes"+i+"` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, "+
						  "CONSTRAINT `target"+i+"` FOREIGN KEY (`target`) REFERENCES `nodes"+i+"` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION "+
						  ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ";
				try{writeStatement.executeUpdate(nodeQuery); } catch (SQLException e){e.printStackTrace();}
				try{writeStatement.executeUpdate(edgeQuery);	} catch (SQLException e){
					e.printStackTrace();}			
				
					
				
				nodeQuery = "";
				edgeQuery = "";
				
				String set = sets.get(i);
				
				Connection readConnection = JDBCMySQLConnection.getConnection(schema);
				Statement readStatement = readConnection.createStatement();
				
				try{
					//Communities
					ResultSet communities = readStatement.executeQuery("SELECT * FROM "+set+" GROUP BY communityID;");					
					while (communities.next()){
						//Community Name
						String communityName = communities.getString("communityID");
						
						//Nodes
						ResultSet nodes = readStatement.executeQuery("SELECT * FROM "+set+" WHERE communityID = "+communityName+";");	
						while (nodes.next()){
							
						}
					}
				}catch(SQLException e){
					e.printStackTrace();
				}
				
				
				
				////////////////////////////////////////////////////////
				//Communities
				for (Map.Entry<String, List<Node>> entry : set.entrySet()){
					List<Node> community = entry.getValue();
					if (community == null)continue;
					//Community Name
					String communityName = entry.getKey();
					//Nodes
					for (Node node : community){
						//Create Edges from Node
						for (Node neighbor : node.getNeighbors()){
							Edge edge = new Edge(node.getId(),neighbor.getId(),1);
							//check if edge is already there
							boolean existing = false;
							for (Edge fromList : edgeList){
								if ((fromList.getSource()==edge.getSource() && fromList.getTarget() == edge.getTarget())
										|| (!directed && fromList.getSource()==edge.getTarget() && fromList.getTarget() == edge.getSource())){
									existing = true;
								}
							}
							if (!existing) edgeList.add(edge);							
						}
						//Add Node to Query
						nodeQuery="INSERT INTO `communitysets"+schemaNumber+"`.`nodes"+i+"` (`id`, `label`,`community`) VALUES ('"+node.getId()+"', '"+node.getLabel()+"', '"+communityName+"');";
						try{writeStatement.executeUpdate(nodeQuery); } catch (SQLException e){e.printStackTrace();}
					}
				}
				//Add Edges to SQL

				for (Edge edge : edgeList){
					edgeQuery = " INSERT INTO `communitysets"+schemaNumber+"`.`edges"+i+"` (`source`, `target`, `weight`) "
							+ "VALUES ('"+edge.getSource()+"', '"+edge.getTarget()+"', '"+edge.getWeight()+"');";
					try{writeStatement.executeUpdate(edgeQuery);} catch (SQLException e){e.printStackTrace();}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (writeConnection != null) {
				try {
					writeConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Saved Nodes and Edges into database: communitysets"+schemaNumber);
	} */
}
