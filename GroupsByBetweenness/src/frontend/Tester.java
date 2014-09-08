package frontend;

import java.util.ArrayList;

import dataProcessing.ComponentHelper;

public class Tester {

	public static void main(String[] args) {
		ComponentHelper ch = new ComponentHelper(new ArrayList(),new ArrayList(),"friendnet");
		ch.resetEdges();
	}

}
