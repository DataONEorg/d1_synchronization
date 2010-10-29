/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.mercury;

/**
 *
 * @author rwaltz
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DBAccessor
{
	 private Connection conn = null;
	 private String url = "";
	 private String driver = "";
	 private String userName = "";
	 private String password = "";
	 private static DBAccessor instance = null;
	 private String dbName = "";


	 public void init()
	 {
	    //System.out.println("New DBAccessor constructor...");
	 	setConnection();
	 }

	  private void setConnection()
	  {
	    conn = null;
	    try
		{
	    	//System.out.println("New connection to be built..." + url);
	    	Class.forName(driver);
	  	  	conn = DriverManager.getConnection(url, userName, password);
	  	  System.out.println("New connection built...");
	    }
		catch(ClassNotFoundException cnfe)
		{
		  System.out.println("Class Not Found Exception " + cnfe);
		  cnfe.printStackTrace();
		}
		catch(SQLException sqle)
		{
		  System.out.println("SQL Exception " + sqle);
		  sqle.printStackTrace();
		}
	  }

	  private boolean checkConnection()
	  {
		  boolean connGood = false;
		  try
			{
			    //System.out.println("Inside check connection...");
				String tsql = "select (CURRENT_TIMESTAMP)";
				//System.out.println(sql);
				Statement tStat = conn.createStatement();
				ResultSet tResultSet = tStat.executeQuery(tsql);
				tResultSet = null;
				tStat = null;
				connGood = true;

			}
			catch(Exception connException)
			{
				System.out.println("Bad connection. Conenction getting replenished...");
				connGood = false;
			}
			return connGood;
	  }

	  public Connection getConnection()
	  {
		  if(!checkConnection())
		  {
			  conn = null;
			  setConnection();
		  }
		  return conn;
	  }

	  private void closeConnection()
	  {
	  	try
	    {
	      conn.close();
	      //System.out.println("Connection is closed ....");
	    }
	    catch (SQLException e)
	    {
	      System.out.println("Can't close connection for pool " +e);
	    }
	  }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }


}
