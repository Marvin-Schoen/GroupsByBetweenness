package frontend;

import java.util.List;
import java.util.Map;

import methods.BetweennessGroups;
import data.Edges;
import data.Nodes;
import dataProcessing.GDFReader;
import dataProcessing.SQLGrabber;
import dataProcessing.ComponentHelper;

public class Executer {
	static List<Nodes> nodeList;
	static List<Edges> edgeList;
	
	public static void main(String[] args){
		GDFReader.GDFtoSQL("C:\\Users\\Marvin\\Desktop\\MarvsFriendNetwork.gdf");
		nodeList=SQLGrabber.grabNodes();
		edgeList=SQLGrabber.grabEdges();	
		BetweennessGroups bg = new BetweennessGroups(nodeList, edgeList);
		List<Map<String,List<Nodes>>> tyler=bg.tyler(4,50,10);
		SQLGrabber.saveSets(tyler, false);
		
		ComponentHelper ch = new ComponentHelper(nodeList, edgeList);
		ch.writeGroupsToFile(tyler,"C:\\Users\\Marvin\\Desktop\\tylerResults.csv");

		System.out.println("Terminated");
	}
}
