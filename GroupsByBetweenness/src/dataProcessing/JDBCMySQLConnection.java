package dataProcessing;

import java.sql.*;
public class JDBCMySQLConnection {
	/////////// Variables //////////////////////////////////////////////////////////
	//static reference to itself
	private static JDBCMySQLConnection instance = new JDBCMySQLConnection();
	//Constant containing the Java driver
	public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
	//Constant for the Database URL (standard port 3306 can be omitted)
	public static final String URL = "jdbc:mysql://localhost:3306/wm20142";
	//Constant for the user name
	public static final String USER = "root";
	//Constant for the user password
	public static final String PASSWORD = "root";
	
	//private constructor
	private JDBCMySQLConnection() {
		try {
			//Load MySQL Java driver
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private Connection createConnection() {

		Connection connection = null;
		try {
			//Establish Java MySQL connection
			connection = DriverManager.getConnection(URL, USER, PASSWORD);
		} catch (SQLException e) {
			System.out.println("ERROR: Unable to Connect to Database.");
		}
		return connection;
	}
	
	public static Connection getConnection() {
		return instance.createConnection();
	}		
}
