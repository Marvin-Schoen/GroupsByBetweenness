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
		GDFReader.GDFtoSQL("C:\\Users\\Marvin\\Desktop\\MarvsFriendNetwork.gdf");
		nodeList=SQLGrabber.grabNodes();
		edgeList=SQLGrabber.grabEdges();	
		BetweennessGroups bg = new BetweennessGroups(nodeList, edgeList);
		//List<Map<String,List<Node>>> tyler=bg.tyler(4,50,10);
		//SQLGrabber.saveSets(tyler, false);
		Centrality centrality = new Centrality(nodeList,edgeList);
		float a = centrality.degreeCentrality(nodeList.get(0), false);
		float b = centrality.closenessCentrality(nodeList.get(0), false);
		float c = centrality.betweennessCentrality(nodeList.get(0), false);
		System.out.println("degree="+a+", closeness="+b+", betweenness="+c);
		ComponentHelper ch = new ComponentHelper(nodeList, edgeList);
		//ch.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\tylerResults.csv");

		System.out.println("Terminated");
	}
}
