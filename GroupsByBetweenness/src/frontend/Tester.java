package frontend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import dataProcessing.ComponentHelper;
import dataProcessing.JDBCMySQLConnection;

public class Tester {

	public static void main(String[] args) {
		Statement statement=null;
		Connection connection = JDBCMySQLConnection.getConnection("friendnet");
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		ResultSet rs=null;
		try{
		rs = statement.executeQuery("SELECT * FROM nodes WHERE id = 1234");
		} catch (SQLException e){
			System.out.println("a query that finds no row throws an SQL exception");
		}
		if (rs==null){
			System.out.println("dijstra: source must be contained in List");
			return;
		}
		System.out.println("Tester Terminated");
	}

}
