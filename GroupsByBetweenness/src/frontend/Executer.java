package frontend;

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
	
	public static void main(String[] args){
		//GDFReader.GDFtoSQL("C:\\Users\\Marvin\\Desktop\\MarvsFriendNetwork.gdf");
		boolean directional = true;
		nodeList=SQLGrabber.grabNodes("wm20142");
		edgeList=SQLGrabber.grabEdges("wm20142");	
		BetweennessGroups bg = new BetweennessGroups(nodeList, edgeList);
		List<Map<String,List<Node>>> tyler=bg.tyler(20,1000,10,directional);
		SQLGrabber.saveSets(tyler, directional);
		Centrality centrality = new Centrality(nodeList,edgeList);
		
		String output = "";
		output += "Label\tdegree\tcloseness\tbetweenness\n";
		System.out.print("Label\tdegree\tcloseness\tbetweenness\n");
		for (Node node : nodeList){
			float a = centrality.degreeCentrality(node, directional);
			float b = centrality.closenessCentrality(node, directional);
			float c = centrality.betweennessCentrality(node, directional);
			
			output+=node.getLabel()+"\t"+a+"\t"+b+"\t"+c+"\n";
			System.out.print(node.getLabel()+"\t"+a+"\t"+b+"\t"+c+"\n");
		}
		ComponentHelper ch = new ComponentHelper(nodeList, edgeList);
		ch.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\tylerResults.csv");
		ch.writeStringToFile(output, "C:\\Users\\Marvin\\Desktop\\centralityResults.csv");
		System.out.println("Terminated");
	}
}
