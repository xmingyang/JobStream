package org.jobstream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbCoonect {
	public static Connection getConnectionMySql() throws Exception {
		Connection c = null;
		try {
			Class.forName(PropHelper.getStringValue("jdbc.driverClassName"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
		//	c = DriverManager
	//				.getConnection(
	//						"jdbc:mysql://10.77.140.148:10036/wanda",
	//						"wanda", "wanda123");
			
			c = DriverManager
					.getConnection(
							PropHelper.getStringValue("jdbc.url"),
							PropHelper.getStringValue("jdbc.username"), PropHelper.getStringValue("jdbc.password"));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return c;
	}

}
