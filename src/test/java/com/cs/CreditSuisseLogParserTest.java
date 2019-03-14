package com.cs;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.junit.Test;

import com.cs.hsqldb.HsqlConnectionManager;


public class CreditSuisseLogParserTest {
	private final static Logger logger = Logger.getLogger(CreditSuisseLogParserTest.class.getName());
	private static String TEST_DATABASE = "test/testDatabaseFile";
 
	@Test
	public void testParser() throws Exception{
		logger.info(">> testParser"); 
		
		String inputFilePath = "src\\test\\resources\\logfile.json";
		
		CreditSuisseLogParser newParser = new CreditSuisseLogParser(inputFilePath, TEST_DATABASE);
		
		//validate records 
		Connection connection = HsqlConnectionManager.createJdbcConnection(TEST_DATABASE);		
		validateTotalRows(connection);
		
		validateRecords(connection);

		cleanUp(connection);
		  
		logger.info("<< testParser");

	}

	private void cleanUp(Connection connection) throws SQLException {
		HsqlConnectionManager.executeUpdateSql(connection, "DELETE FROM LOGRESULT");
		HsqlConnectionManager.closeJdbcConnection(connection);
		HsqlConnectionManager.shutdown(this.TEST_DATABASE);
	}

	private void validateRecords(Connection connection) throws SQLException {
		ResultSet theResult = HsqlConnectionManager.executeQuerySql(connection, "SELECT eventid, duration, eventhost, isAlert, eventtype FROM LOGRESULT order by eventid desc");
 
		validateEvent(theResult, "scsmbstgrc", "", 8 , "", true);
		validateEvent(theResult, "scsmbstgrb", "", 3 , "", false);
		validateEvent(theResult, "scsmbstgra", "APPLICATION_LOG", 5 , "12345", true); 
	}

	private void validateEvent(ResultSet theResult, String eventId, String eventType, int duration, String eventHost, boolean isAlert) throws SQLException {
		theResult.next(); 
		assertEquals(theResult.getString("eventid"), eventId);
		assertEquals(theResult.getString("eventtype"), eventType);
		assertEquals(theResult.getInt("duration"), duration); 
		assertEquals(theResult.getString("eventhost"), eventHost);
    	assertEquals(theResult.getBoolean("isalert"), isAlert);
	}

	private void validateTotalRows(Connection connection) throws SQLException {
		ResultSet countResult = HsqlConnectionManager.executeQuerySql(connection, "SELECT COUNT(*) AS TOTALROWS FROM LOGRESULT");
  
		countResult.next();	
		assertEquals(3, countResult.getInt("TOTALROWS"));
	}

}
