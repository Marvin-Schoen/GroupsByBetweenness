package dataProcessing;

import dataProcessing.JDBCMySQLConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
	
	public static void saveSets(List<List<List<Nodes>>> sets,boolean directed){
		//Establish connections
		Connection connection = null;
		Statement statement = null;		
		try {
			connection = JDBCMySQLConnection.getConnection();
			statement = connection.createStatement();
			//Tables
			for (int i = 0;i<sets.size();i++){
				//Variables
				List<Edges> edgeList = new ArrayList<Edges>();
				//Create Tables for Set i
				String nodeQuery = 	"CREATE TABLE `friendnet`.`nodes"+i+"` ("+
									"`id` INT NOT NULL,"+
									"`label` VARCHAR(45) NULL,"+
									"PRIMARY KEY (`id`));";
				
				String edgeQuery = 	"CREATE TABLE `friendnet`.`edges"+i+"` ("+
						  			"`source` DOUBLE NOT NULL,"+
				  					"`target` DOUBLE NOT NULL,"+
				  					"`weight` INT NULL,"+
				  					"PRIMARY KEY (`source`, `target`),"+
				  					"INDEX `target_idx` (`target` ASC),"+
				  					"CONSTRAINT `source`"+
				  					"FOREIGN KEY (`source`)"+
				  					"REFERENCES `friendnet`.`nodes"+i+"` (`id`)"+
				  					"ON DELETE NO ACTION"+
				  					"ON UPDATE NO ACTION,"+
				  					"CONSTRAINT `target`"+
				  					"FOREIGN KEY (`target`)"+
				  					"REFERENCES `friendnet`.`nodes"+i+"` (`id`)"+
				  					"ON DELETE NO ACTION"+
				  					"ON UPDATE NO ACTION);";
				try{
					statement.executeUpdate(nodeQuery);
					statement.executeUpdate(edgeQuery);				
				} catch (SQLException e){
					e.printStackTrace();
				}
				nodeQuery = "";
				edgeQuery = "";
				
				List<List<Nodes>> set = sets.get(i);
				//Communities
				for (List<Nodes> community : set){
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
						nodeQuery+=" INSERT INTO `friendnet`.`nodes"+i+"` (`id`, `label`) VALUES (`"+node.getId()+"`, `"+node.getLabel()+"`);";
				
					}
				}
				//Add Edges to SQL

				for (Edges edge : edgeList){
					edgeQuery += " INSERT INTO `friendnet`.`edges"+i+"` (`source`, `target`, `weight`) "
							+ "VALUES ('"+edge.getSource()+"', '"+edge.getTarget()+"', '"+edge.getWeight()+"');";
				}
				try{
					statement.executeUpdate(edgeQuery);			
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
