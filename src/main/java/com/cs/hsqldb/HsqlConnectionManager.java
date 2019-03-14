package com.cs.hsqldb;

import java.sql.Statement;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
 

import com.cs.CreditSuisseLogParser;

public class HsqlConnectionManager {
	private final static Logger logger = Logger.getLogger(HsqlConnectionManager.class.getName());

	public static Connection createJdbcConnection(String databaseName) throws ClassNotFoundException, SQLException  {
		logger.info(">> createJdbcConnection : databaseName " + databaseName );
		 
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
	  
		// Creating the connection with HSQLDB
		Connection connection = DriverManager.getConnection("jdbc:hsqldb:file:" + databaseName, "SA", "");
		 
		logger.info("<< createJdbcConnection : databaseName " + databaseName );
		return connection;
	}
	
	public static void closeJdbcConnection (Connection connection) throws SQLException {
		logger.info(">> closeJdbcConnection ");
		connection.prepareStatement("SHUTDOWN").execute();
		connection.close();				
		logger.info("<< closeJdbcConnection ");
	}
	
	 
	public static void shutdown (String databaseName) throws SQLException {
		logger.info(">> shutdown database");
		Connection connection = DriverManager.getConnection("jdbc:hsqldb:file:" + databaseName + ";shutdown=true", "SA", "");
		logger.info("<< shutdown database ");
		
	}
	public static ResultSet executeQuerySql(Connection connection, String sql) throws SQLException{
		logger.info(">> executeQuerySql : sql " + sql );
		ResultSet resultSet = null;		 
		Statement statement = connection.createStatement();
		try { 
			resultSet = statement.executeQuery(sql);
 		} catch (Exception e) {
			logger.info("Error running SQL query " + e);
		} finally {
			statement.close();
			logger.info("<< executeQuerySql :  success ");
		}		
		return resultSet;
	}
	
	
	
	public static void executeUpdateSql(Connection connection, String sql) throws SQLException{
		logger.info(">> executeUpdateSql : sql " + sql );
		 
		Statement statement = connection.createStatement();
		int result = 0;
		try { 
			result = statement.executeUpdate(sql);
		 
		} catch (Exception e) {
			logger.info("Error running SQL query " + e);
		}  
		logger.info(result + " rows updated / deleted");			
	}
	 
	
	public static void createTable(Connection connection, String tableName) throws Exception{
		logger.info(">> createTable : tableName = " + tableName );		
		DatabaseMetaData dbm = connection.getMetaData();
		// check if "logresult" table is there
		ResultSet tables = dbm.getTables(null, null, tableName, null);
		boolean tableExists = false;
		while (tables.next()) {
			String tName = tables.getString("TABLE_NAME");
			if (tName != null && tName.equals("LOGRESULT")) {
				tableExists = true;
				break;
			}
		}

		if (tableExists) {			 
			logger.info("The table already exists no need to create. TableName = " + tableName);
		} else {			 
			Statement stmt = connection.createStatement();
			int result = stmt.executeUpdate(
					"CREATE TABLE logresult (eventid VARCHAR(50) NOT NULL, duration INT NOT NULL, eventhost VARCHAR(20), eventtype VARCHAR(20), isAlert INT NOT NULL); ");

			connection.commit();
		}		
	}

}
