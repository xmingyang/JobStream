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
			
			c = DriverManager
					.getConnection(
							PropHelper.getStringValue("jdbc.url"),
							PropHelper.getStringValue("jdbc.username"), PropHelper.getStringValue("jdbc.password"));
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return c;
	}
	
	public static Connection getConnectionMySql_retry() throws Exception {
		Connection c = null;
		//数据连接失败30s后重试，最多重试60次
		for (int retrynum=1;retrynum<=60;retrynum++)
		{
			try {
			c=getConnectionMySql();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
			if (c!=null)
				return c;
			else
				Thread.sleep(30000);
		}
		return c;
	}
	


}
