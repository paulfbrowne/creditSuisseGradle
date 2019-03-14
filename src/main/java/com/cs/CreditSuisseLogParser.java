package com.cs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.cs.hsqldb.HsqlConnectionManager;
import com.cs.model.JsonRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

public class CreditSuisseLogParser {
	private final static Logger logger = Logger.getLogger(CreditSuisseLogParser.class.getName());

	private final static String RUNTIME_DATABASE = "runtime/logfile";

	public static void main(String[] args) {
		if (args.length > 0) {

			CreditSuisseLogParser newParse = new CreditSuisseLogParser(args[0], RUNTIME_DATABASE);
		} else {
			System.out.println("Please give the name of the logfile to use as a param");
			logger.info("Please give the name of the logfile to use as a param");
		}
	}

	public CreditSuisseLogParser(String logfile, String databaseName) {
		logger.info(">> CreditSuisseLogParser logfile " + logfile + ", databaseName " + databaseName);
		if (logfile != null && logfile.length() > 0) {

			File file = new File(logfile);
			if (!file.exists() || file.isDirectory()) {
				System.out.println("Cannot find logfile " + logfile + ",");
				logger.info("Cannot find logfile " + logfile + ",");
				System.exit(0);
			}
		}

		 
		logger.info("processing logfile " + logfile + ",");

		try {
			Connection connection = HsqlConnectionManager.createJdbcConnection(databaseName);
			HsqlConnectionManager.createTable(connection, "LOGRESULT");

			readStream(logfile, connection);
 
			HsqlConnectionManager.closeJdbcConnection(connection);
			HsqlConnectionManager.shutdown(this.RUNTIME_DATABASE);

		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		logger.info("<< CreditSuisseLogParser");

	}

	public int readStream(String logfile, Connection connection) {
		logger.info(">> readStream logfile " + logfile);
		 
		int recordsInsertedIntoDB=0;
		Map<String, JsonRecord[]> logdata = new HashMap<String, JsonRecord[]>();

		try {

			FileInputStream thefile = new FileInputStream(logfile);
			JsonReader reader = new JsonReader(new InputStreamReader(thefile, "UTF-8"));

			Gson gson = new GsonBuilder().create(); 
			
			// Read file in stream mode
			reader.beginArray();
			while (reader.hasNext()) {
				// Read data into object model
				JsonRecord therecord = gson.fromJson(reader, JsonRecord.class);
				 
				if (logdata.containsKey(therecord.getId())) {
					JsonRecord therecords[] = new JsonRecord[2];
					therecords = logdata.remove(therecord.getId());
					if (therecord.getState().compareTo("STARTED") == 0) {
						therecords[0] = therecord;
					} else {
						therecords[1] = therecord;
					}

					if (therecords[0] != null && therecords[1] != null) {
						long duration = therecords[1].getTimestamp() - therecords[0].getTimestamp();
						 
						String eventid = therecords[1].getId();
						String host = therecords[1].getHost();
						String state = therecords[1].getState();
						String type = therecords[1].getType();
 
						if (duration > 4) { 
							HsqlConnectionManager.executeUpdateSql(connection, "INSERT INTO logresult VALUES ('"
									+ eventid + "'," + duration + ",'" + host + "', '" + type + "', 1)");
						} else { 
							HsqlConnectionManager.executeUpdateSql(connection, "INSERT INTO logresult VALUES ('"
									+ eventid + "'," + duration + ",'" + host + "', '" + type + "', 0)");

						}

						recordsInsertedIntoDB++;	
						logdata.remove(therecord.getId());
					} else {
						logdata.put(therecord.getId(), therecords);
					}
				} else {
					JsonRecord therecords[] = new JsonRecord[2];
					if (therecord.getState().compareTo("STARTED") == 0) {
						therecords[0] = therecord;
						therecords[1] = null;
					} else {
						therecords[1] = therecord;
						therecords[0] = null;
					}
					logdata.put(therecord.getId(), therecords);
				} 
			}
			reader.close();
		} catch (UnsupportedEncodingException ex) {
			System.out.println("UnsupportedEncodingException");
		} catch (IOException ex) {
			System.out.println("Problem IOException");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("<< readStream"  );		 
		return recordsInsertedIntoDB;
	}

}
