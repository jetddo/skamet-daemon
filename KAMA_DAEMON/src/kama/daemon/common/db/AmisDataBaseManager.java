package kama.daemon.common.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class AmisDataBaseManager {

	private Connection connect;
	private Statement statement;
	private ResultSet resultSet;

	public AmisDataBaseManager(Configuration config) {
		
		StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("pwkey");
		
		String username = config.getString("db.amis.user");
		String password = encryptor.decrypt(config.getString("db.amis.password"));
		String url = config.getString("db.amis.url");
		
		try {
			
			//Class.forName("oracle.jdbc.driver.OracleDriver");

			this.connect = DriverManager.getConnection(url, username, password);
			this.statement = this.connect.createStatement();

		} catch (SQLException e) {

			System.out.println("DataBaseManager -> " + e.getMessage());

			this.safeClose();
		}
	}

	/**
	 * Auto Commit ON / OFF
	 * @param flag
	 */
	public void setAutoCommit(boolean flag) {

		try {

			this.connect.setAutoCommit(flag);

		} catch (SQLException e) {
			System.out.println("DataBaseManager.setAutoCommit -> " + e.getMessage());
		}
	}

	/**
	 * Database Commit
	 */
	public void commit() {

		try {

			this.connect.commit();
			System.out.println("[ Commit Completed! ]");

		} catch (SQLException e) {
			System.out.println("DataBaseManager.commit -> " + e.getMessage());
		}
	}

	/**
	 * DataBase RollBack
	 */
	public void rollback() {

		try {

			this.connect.rollback();
			System.out.println("[ Rollback Completed! ]");

		} catch (SQLException e) {
			System.out.println("DataBaseManager.rollback -> " + e.getMessage());
		}
	}

	/**
	 * Execute Insert Query
	 * @param query
	 * @return
	 */
	public int insert(String query) {

		try {

			return this.statement.executeUpdate(query);

		} catch (SQLException e) {
			
			System.out.println("DataBaseManager.insert -> " + e.getMessage());

			return -1;
		}
	}

	/**
	 * Execute Delete Query	
	 * @param query
	 * @return
	 */
	public int delete(String query) {

		try {

			return this.statement.executeUpdate(query);

		} catch (SQLException e) {
			
			System.out.println("DataBaseManager.delete -> " + e.getMessage());

			return -1;
		}
	}
	
	/**
	 * Execute Select Query - Map<String, Object>
	 * @param query
	 * @return
	 */
	public List<Map<String, Object>> select(String query) {

		List<Map<String, Object>> resultList = new ArrayList <Map<String, Object>> ();

		try {

			this.resultSet = this.statement.executeQuery(query);

			while (this.resultSet.next()) {
				resultList.add(DaemonUtils.getCamelcaseResultSetData(this.resultSet));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DataBaseManager.select -> " + e.getMessage());
		}

		return resultList;
	}
	
	/**
	 * Execute Select Query - Map<String, Object>
	 * @param query
	 * @return
	 */
	public List<Map<String, Object>> selectWithCamelcase(String query) {

		List<Map<String, Object>> resultList = new ArrayList <Map<String, Object>> ();

		try {

			this.resultSet = this.statement.executeQuery(query);

			while (this.resultSet.next()) {
				resultList.add(DaemonUtils.getCamelcaseResultSetData(this.resultSet));
			}

		} catch (SQLException e) {
			System.out.println("DataBaseManager.select -> " + e.getMessage());
		}

		return resultList;
	}

	public void safeClose() {

		try {

			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}

		} catch (Exception e) {}
	}
}