package data;
/**
 * Edges class represents the notes for the Gephi Toolkit
 * @author Marvin
 * @see https://wiki.gephi.org/index.php/Toolkit_-_Import_from_RDBMS
 *
 */
public class Edge {
	private String source;
	private String target;
	private float weight;
	public Edge(String s,String t, int w){
		setSource(s);
		setTarget(t);
		setWeight(w);
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public float getWeight() {
		return weight;
	}
	public void setWeight(float weight) {
		this.weight = weight;
	}
	
	//toString()
	@Override
	public String toString() {
		return "Edge [Source=" + source + ", Target=" + target + ", weight="
				+ weight + "]";
	}	
}
