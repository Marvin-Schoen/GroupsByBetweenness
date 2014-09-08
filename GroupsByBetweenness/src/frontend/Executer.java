package frontend;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import methods.BetweennessGroups;
import methods.Centrality;
import data.Edge;
import data.Node;
import dataProcessing.GDFReader;
import dataProcessing.SQLGrabber;
import dataProcessing.ComponentHelper;

public class Executer {
	static List<Node> nodeList;
	static List<Edge> edgeList;
	private static final int TYLER = 1;
	private static final int DEGREE = 2;
	private static final int CLOSENESS = 3;
	private static final int BETWEENNESS = 4;
	
	private static final int SCHEMA = 1;
	private static final int THRESHOLD = 2;
	private static final int NUMBEROFSETS = 3;
	private static final int SEED = 4;
	private static final int DIRECTIONAL = 5;
	
	public static void main(String[] args){
		//GDFReader.GDFtoSQL("C:\\Users\\Marvin\\Desktop\\MarvsFriendNetwork.gdf");
		boolean directional = false;
		String schema= "friendnet";
		double threshold = Double.POSITIVE_INFINITY;
		int tylerRepititions = 10;
		int seed = 10;
		int method = 0;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		boolean properInput = true;
		String methodName = "";
        do {	   
        	properInput = true;
	        System.out.println("Welcome to Marvin Schoens iCup tool. Please Choose you method.");
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
		
		nodeList=SQLGrabber.grabNodes(schema);
		edgeList=SQLGrabber.grabEdges(schema);	
		BetweennessGroups bg = new BetweennessGroups(nodeList, edgeList,schema);
		ComponentHelper ch = new ComponentHelper(schema);
		Centrality centrality = new Centrality(nodeList,edgeList,schema);		
		
		//reset edge status. None of them is deleted
		ch.resetEdges();
		
		String output = "";
		if (method == TYLER){
			List<Map<String,List<Node>>> tyler=bg.tyler(tylerRepititions,threshold,seed,directional);		
			SQLGrabber.saveSets(tyler, directional);		
			Date dNow = new Date( );
			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
			ch.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"tylerResults.csv");
		} else {			
			if (method == DEGREE){
				output += "Label\tdegree\n";
				for (Node node : nodeList){
					float a = centrality.degreeCentrality(node);
					output+=node.getLabel()+"\t"+a+"\n";
				}
			}
			
			else if (method == CLOSENESS){
				output += "Label\tcloseness\n";
				for (Node node : nodeList){
					float a = centrality.closenessCentrality(node,directional);
					output+=node.getLabel()+"\t"+a+"\n";
				}
			}
			
			else if (method == BETWEENNESS){
				output += "Label\tbetweenness\n";
				for (Node node : nodeList){
					float a = centrality.betweennessCentrality(node,directional);
					output+=node.getLabel()+"\t"+a+"\n";
				}
			}
			
			Date dNow = new Date( );
			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
			ch.writeStringToFile(output, "C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"centralityResults.csv");
		}
		
		
		System.out.println("Terminated");
	}
}
