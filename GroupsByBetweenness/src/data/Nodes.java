package data;

import java.util.ArrayList;
import java.util.List;

/**
 * Nodes class represents the notes for the Gephi Toolkit
 * @author Marvin
 * @see https://wiki.gephi.org/index.php/Toolkit_-_Import_from_RDBMS
 *
 */
public class Nodes implements Comparable<Nodes> {
	private String id;
	private String label;
	List<Nodes> neighbors;
	// For Dijkstra Distance to the observed node
	private double distance = Double.POSITIVE_INFINITY;
	//For Dijkstra node that leads the shortest path to the observed node
	private Nodes previous;

	public Nodes(String i, String l){
		setId(i);
		setLabel(l);
		neighbors = new ArrayList<Nodes>();
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double tentativeDistance) {
		this.distance = tentativeDistance;
	}
	public void addNeighbor(Nodes n){
		neighbors.add(n);
	}
	public List<Nodes> getNeighbors(){
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
    public int compareTo(Nodes other)
    {
        return Double.compare(getDistance(), other.getDistance());
    }
	//toString()
	@Override
	public String toString() {
		return "Node [ID=" + id + ", Label=" + label + ", distance=" + distance+  "]";
	}
	public Nodes getPrevious() {
		return previous;
	}
	public void setPrevious(Nodes previous) {
		this.previous = previous;
	}	
}
