package frontend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import methods.BetweennessGroups;
import methods.BetweennessGroupsNoSQL;
import methods.Centrality;
import methods.CentralityNoSQL;
import data.Edge;
import data.Node;
import dataProcessing.ComponentHelperNoSQL;
import dataProcessing.JDBCMySQLConnection;
import dataProcessing.ComponentHelper;
import dataProcessing.SQLGrabber;

public class Executer {
	private static final int TYLER = 1;
	private static final int DEGREE = 2;
	private static final int CLOSENESS = 3;
	private static final int BETWEENNESS = 4;
	
	private static final int SCHEMA = 1;
	private static final int THRESHOLD = 2;
	private static final int NUMBEROFSETS = 3;
	private static final int SEED = 4;
	private static final int DIRECTIONAL = 5;
	
	private static Connection connection;
	private static Statement statement;
	
	static Map<String,Node> nodeList;
	static Map<String,Edge> edgeList;
	
	public static void main(String[] args){
		//GDFReader.GDFtoSQL("C:\\Users\\Marvin\\Desktop\\MarvsFriendNetwork.gdf");
		boolean directional = false;
		String schema= "wm2014_cleaned";
		double threshold = 400;//Double.POSITIVE_INFINITY;
		int tylerRepititions = 1;
		int seed = 10;
		int method = 0;
		boolean useSQL = false;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		connection = JDBCMySQLConnection.getConnection(schema);
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		boolean properInput = true;
			
		do{
			properInput = true;
			System.out.println("Welcome to Marvin Schoens iCup tool. Please Choose if you want to work on main memory(0) or hard drive(1).");
			int input=0;
			try{
	            input = Integer.parseInt(br.readLine());
	        }catch(NumberFormatException nfe){
	            System.err.println("Please enter a number!");
	            properInput =false;
	        } catch (IOException e) {
				e.printStackTrace();
			}  
			
			if (input==0){
				useSQL = false;
			} else if (input == 1){
				useSQL = true;
			} else {
				properInput = false;
			}
		}while (!properInput);
		
		String methodName = "";
        do {	   
        	properInput = true;
	        System.out.println("Please Choose you method.");
	        System.out.println(TYLER+": Tylers betweenness groups");
	        System.out.println(DEGREE+": Degree centrality");
	        System.out.println(CLOSENESS+": Closeness centrality");
	        System.out.println(BETWEENNESS+": Betweenness centrality");
	        
	        
	        try{
	            method = Integer.parseInt(br.readLine());
	        }catch(NumberFormatException nfe){
	            System.err.println("Please enter a number!");
	        } catch (IOException e) {
				e.printStackTrace();
			}  
        	
        	switch (method){
        	case TYLER: methodName = "Tylers betweenness groups"; break;
        	case DEGREE: methodName = "Degree centrality"; break;
        	case CLOSENESS: methodName ="Closeness centrality"; break;
        	case BETWEENNESS: methodName ="Betweenness centrality"; break;
        	default: properInput = false;
        	}
        	if (properInput)
        		System.out.println("You chose "+ methodName + " do you want to continue with the standard parameters or enter your own? The standard parameters are:");
        
        } while (!properInput);
        
        do{
	        System.out.println(SCHEMA+": Schema is "+schema);
	        System.out.println(THRESHOLD+": Threshold for the tyler algorithm is "+threshold);
	        System.out.println(NUMBEROFSETS+": Number of repetitions for the tyler algorithm is "+tylerRepititions);
	        System.out.println(SEED+": The seed for the random generator is "+seed);
	        System.out.println(DIRECTIONAL+": The network is directional: "+directional);
	        System.out.println("Please enter the number of the parameter you want to change or enter 0 to continue");
        
	        int param = 0;       
        
	        try{
	            param = Integer.parseInt(br.readLine());
	        }catch(NumberFormatException nfe){
	            System.err.println("Please enter a number!");
	        } catch (IOException e) {
				e.printStackTrace();
			}

        	if (param == 0) break;
        	properInput = true;
        	System.out.println("Change the parameter to:");
        	String value="";
        	try{
                value = br.readLine();
            } catch (IOException e) {
    			e.printStackTrace();
    		}
        	switch (param){
        	case SCHEMA: schema = value; 
        		properInput = false;
        		break;
        	case THRESHOLD: try {
	        		threshold = Double.parseDouble(value);}
	        	catch(NumberFormatException nfe){
	                System.err.println("Please enter a number!");
	            } 
        		properInput = false;
        		break;
        	case NUMBEROFSETS: try {
        		tylerRepititions = Integer.parseInt(value);}
	        	catch(NumberFormatException nfe){
	                System.err.println("Please enter a number!");	                
	            }
        		properInput = false;
        		break;
        	case SEED: try {
        		seed = Integer.parseInt(value);}
	        	catch(NumberFormatException nfe){
	                System.err.println("Please enter a number!");
	            } 
        		properInput = false;
        		break;
        	case DIRECTIONAL: directional = "true".equals(value); 
        		properInput = false;
        		break;
        	default: properInput = false;
        	}
        	
        	
        } while (!properInput);
        
        //Continue form last State?
        boolean cont = false;
    	if (method == TYLER && useSQL){
    		 do{
    			 properInput=false;
    			 System.out.println("Continue from latest state? (1/0)");
    			 int param = 2;
    			 try{
    				 param = Integer.parseInt(br.readLine());
    			 }catch(NumberFormatException nfe){
    				 System.err.println("Please enter a number!");
    			 } catch (IOException e) {
    				 e.printStackTrace();
    			 }
    			 if(param==0){
    				 properInput=true;
    			 } else if (param == 1){
    				 cont = true;
    				 properInput=true;
    			 }
    		 } while (!properInput);
    	}
        
		
		nodeList=SQLGrabber.grabNodes(schema);
		edgeList=SQLGrabber.grabEdges(schema);	
		BetweennessGroups bg = new BetweennessGroups(schema,seed);		
		BetweennessGroupsNoSQL bgNSQL = new BetweennessGroupsNoSQL(nodeList, edgeList);
		ComponentHelper ch = new ComponentHelper(schema,seed);
		ComponentHelperNoSQL chNSQL = new ComponentHelperNoSQL(nodeList, edgeList);
		Centrality centrality = new Centrality(schema,seed);	
		CentralityNoSQL centralityNSQL = new CentralityNoSQL(nodeList, edgeList);
		
		//reset edge status. None of them is deleted
		//ch.resetEdges(); //ONlY needed in harddisk mode
		
		String output = "";
		if (method == TYLER){
			if (useSQL){
				List<String> tyler=bg.tyler(tylerRepititions,threshold,seed,directional,cont);		
				//SQLGrabber.saveSets(tyler, directional);		//would produce to many data
				Date dNow = new Date( );
				SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
				ch.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"tylerResults.csv");
			} else {
				List<Map<String,List<Node>>> tyler = bgNSQL.tyler(tylerRepititions, threshold, seed, directional);
				SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
				Date dNow = new Date( );
				SQLGrabber.saveSets(tyler, directional);								
				chNSQL.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"tylerResults.csv");
			}
		} else {	
			chNSQL.assignNeighbors(directional);
			output += "sep=\t \n";
			if (method == DEGREE){
				output += "Label\tdegree\n";
				if (useSQL){
					try {
						ResultSet node = statement.executeQuery("SELECT * FROM nodes");
						while (node.next()){
							float a = centrality.degreeCentrality(node.getString("nodeID"),directional);
							output+=node.getString("label")+"\t"+a+"\n";
						}
					} catch (SQLException e){
						e.printStackTrace();
					}
				} else {
					for (Node node : nodeList.values()){
						float a = centralityNSQL.degreeCentrality(node);
						output+=node.getLabel()+"\t"+a+"\n";
					}
				}
			}
			
			else if (method == CLOSENESS){
				output += "Label\tcloseness\n";
				if (useSQL){
					try {
						ResultSet node = statement.executeQuery("SELECT * FROM nodes");
						while (node.next()){
							float a = centrality.closenessCentrality(node.getString("nodeID"),directional);
							output+=node.getString("label")+"\t"+a+"\n";
						}
					} catch (SQLException e){
						e.printStackTrace();
					}
				} else {
					for (Node node : nodeList.values()){
						float a = centralityNSQL.closenessCentrality(node,directional);
						output+=node.getLabel()+"\t"+a+"\n";
					}
				}
			}
			
			else if (method == BETWEENNESS){
				output += "Label\tbetweenness\n";
				if (useSQL){
					try {
						ResultSet node = statement.executeQuery("SELECT * FROM nodes");
						while (node.next()){
							float a = centrality.betweennessCentrality(node.getString("nodeID"),directional);
							output+=node.getString("label")+"\t"+a+"\n";
						}
					} catch (SQLException e){
						e.printStackTrace();
					}
				} else {
					for (Node node : nodeList.values()){
						float a = centralityNSQL.betweennessCentrality(node,directional);
						output+=node.getLabel()+"\t"+a+"\n";
					}
				}
			}
			
			Date dNow = new Date( );
			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
			ch.writeStringToFile(output, "C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"centralityResults.csv");
		}
		
		
		System.out.println("Terminated");
	}
}
