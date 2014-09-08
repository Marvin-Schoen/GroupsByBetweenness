package data;

import java.util.ArrayList;
import java.util.List;

/**
 * Nodes class represents the notes for the Gephi Toolkit
 * @author Marvin
 * @see https://wiki.gephi.org/index.php/Toolkit_-_Import_from_RDBMS
 *
 */
public class Node implements Comparable<Node> {
	private String id;
	private String label;
	List<Node> neighbors;
	// For Dijkstra Distance to the observed node
	private double distance = Double.POSITIVE_INFINITY;
	//For Dijkstra node that leads the shortest path to the observed node
	private List<Node> previous;
	
	public Node(String i, String l, double distance){
		setId(i);
		setLabel(l);
		neighbors = new ArrayList<Node>();
		setDistance(distance);
	}
	
	public Node(String i, String l){
		setId(i);
		setLabel(l);
		neighbors = new ArrayList<Node>();
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double tentativeDistance) {
		this.distance = tentativeDistance;
	}
	public void addNeighbor(Node n){
		neighbors.add(n);
	}
	
	public void setNeighbors(List<Node> list){
		this.neighbors=list;
	}
	
	public List<Node> getNeighbors(){
		return neighbors;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	
	//for Dijkstra
    public int compareTo(Node other)
    {
        return Double.compare(getDistance(), other.getDistance());
    }
	//toString()
	@Override
	public String toString() {
		return "Node [ID=" + id + ", Label=" + label + ", distance=" + distance+  "]";
	}
	public List<Node> getPrevious() {
		return previous;
	}
	public void addPrevious(Node previous) {
		if (this.previous==null)
			this.previous=new ArrayList<Node>();
		this.previous.add(previous);
	}
	/**
	 * sets the previous to null
	 */
	public void voidPrevious(){
		this.previous=null;
	}
}
