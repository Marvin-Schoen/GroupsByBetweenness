package frontend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import dataProcessing.ComponentHelper;
import dataProcessing.JDBCMySQLConnection;

public class Tester {
	static class Simple{
		private String word;
		public Simple(String word){
			this.setWord(word);
		}
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
	}

	public static void main(String[] args) {
		ArrayList<Simple> a = new ArrayList<Simple>();
		Simple hello = new Simple("Hello");
		a.add(hello);
		Simple world = new Simple("World");
		a.add(world);
		ArrayList<Simple> b = new ArrayList<Simple>(a); //otherwise b ist just a pointer to a
		Simple first = b.get(0);
		first.setWord("Hello2"); //proves that b points on the same objects as a and not at copies
		for (Simple i : a){
			System.out.println(i.getWord());
		}
	}

}
