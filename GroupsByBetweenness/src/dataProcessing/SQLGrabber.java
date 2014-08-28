package dataProcessing;

import dataProcessing.JDBCMySQLConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import data.Edges;
import data.Nodes;

public class SQLGrabber {
	
	public static List<Nodes> grabNodes(){
		//Empty Variables
		List<Nodes> nodeList = new ArrayList<Nodes>();
		ResultSet rs = null;
		Connection connection = null;
		Statement statement = null; 
		String query = "SELECT * FROM Nodes";
		try {			
			connection = JDBCMySQLConnection.getConnection();
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			
			while(rs.next()){
				nodeList.add(new Nodes(rs.getString("id"),rs.getString("label")));
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
	
	public static List<Edges> grabEdges(){
		//Empty Variables
		List<Edges> edgeList = new ArrayList<Edges>();
		ResultSet rs = null;
		Connection connection = null;
		Statement statement = null; 
		String query = "SELECT * FROM Edges";
		try {			
			connection = JDBCMySQLConnection.getConnection();
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			
			while(rs.next()){
				edgeList.add(new Edges(rs.getString("source"),rs.getString("target"),rs.getInt("weight")));
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
	
	public static void saveSets(List<Map<String,List<Nodes>>> sets,boolean directed){
		//Establish connections
		Connection connection = null;
		Statement statement = null;		
		try {
			connection = JDBCMySQLConnection.getConnection();
			statement = connection.createStatement();
			//Create new Schemaint
			int schemaNumber = 0;
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
				List<Edges> edgeList = new ArrayList<Edges>();
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
				
				Map<String,List<Nodes>> set = sets.get(i);
				//Communities
				for (Map.Entry<String, List<Nodes>> entry : set.entrySet()){
					List<Nodes> community = entry.getValue();
					if (community == null)continue;
					//Community Name
					String communityName = entry.getKey();
					//Nodes
					for (Nodes node : community){
						//Create Edges from Node
						for (Nodes neighbor : node.getNeighbors()){
							Edges edge = new Edges(node.getId(),neighbor.getId(),1);
							//check if edge is already there
							boolean existing = false;
							for (Edges fromList : edgeList){
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

				for (Edges edge : edgeList){
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
	}
}
