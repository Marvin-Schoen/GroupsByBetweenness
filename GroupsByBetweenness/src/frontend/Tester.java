package frontend;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import java.util.List;
import java.util.Map;

import methods.BetweennessGroupsNoSQL;
import data.Node;

import dataProcessing.ComponentHelperNoSQL;

import dataProcessing.SQLGrabber;

public class Tester {

	public static void main(String[] args) {
		Map<String,List<Node>> nodes = SQLGrabber.grabNodesWithCommunity("compare1");
		Map<String,List<Node>> nodes1 = SQLGrabber.grabNodesWithCommunity("compare");
		List<Map<String,List<Node>>> sets = new ArrayList<Map<String,List<Node>>>();
		sets.add(nodes);
		sets.add(nodes1);
		BetweennessGroupsNoSQL bg = new BetweennessGroupsNoSQL(null, null);
		sets=bg.equalComNames(sets);
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss");
		Date dNow = new Date( );
		ComponentHelperNoSQL chNSQL= new ComponentHelperNoSQL(SQLGrabber.grabNodes("compare"),null);
		chNSQL.writeGroupsToFile(sets,"C:\\Users\\Marvin\\Desktop\\"+ft.format(dNow)+"tylerResults.csv");
	}

}
