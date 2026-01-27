package kama.daemon.common.db;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.Log;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

/**
 * @author chlee
 * Created on 2016-11-18. (modified version of jetddo)
 * 데이터 처리 클래스
 */
public class DatabaseManager implements AutoCloseable
{
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    // Instantiate DatabaseManager only once
    private static DatabaseManager s_instance = new DatabaseManager();

    // Database url, username, and password
    private String _url = null;
    private String _user = null;
    private String _password = null;

    private Connection _conn = null;
    private Statement _stmt = null;
    private List<PreparedStatement> _lstpplStmt = null;
    private ResultSet _rs = null;

    private boolean _isInitialized = false;
    private boolean _isStmtPrepared = false;

    public DaemonSettings Settings;

    /**
     * Retrieve current instance of DatabaseManager
     * @return An active instance of DatabaseManager
     */
    public static DatabaseManager getInstance()
    {
        return DatabaseManager.s_instance;
    }

    /**
     * Configure DB using configuration object
     * @param config Settings object
     */
    public void setConfig(DaemonSettings config)
    {
    	StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("pwkey");
    	
        _url = config.DB_url();
        _user = config.DB_user();
        _password = encryptor.decrypt(config.DB_password());

        _lstpplStmt = new ArrayList<PreparedStatement>();
        
        try
        {        
        	
            Class.forName(ORACLE_DRIVER);

            // open database
            _conn = DriverManager.getConnection(_url, _user, _password);
            _stmt = _conn.createStatement();
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            throw new DaemonException("Error : Failed to open database.", false);
        }

        Settings = config;
        _isInitialized = true;
    }

    public void executeUpdate(String query)
    {
        executeUpdate(query, true);
    }

    public void executeUpdate(String query, boolean enableLog)
    {
        _checkDBOpen();

        try
        {
            //System.out.println(query);
            _stmt.executeUpdate(query);
        }
        catch (SQLException ex)
        {
            if (enableLog)
            {
                Log.print(String.format("Error : SQL execution failed. [%s]\n%s", query, ex.toString()));
            }
        }
    }
    
    public ResultSet executeQuery(String query)
    {
        _checkDBOpen();
        
        ResultSet rs = null;

        try
        {
        	rs = _stmt.executeQuery(query);
        }
        catch (SQLException ex)
        {
        	ex.printStackTrace();
            Log.print(String.format("Error : SQL execution failed. [%s]", query));
        }
        
        return rs;
    }

	public PreparedStatement getUnmanagedPrepareStatement(final String sql) {
		_checkDBOpen();

		try {
			return this._conn.prepareStatement(sql);
		} catch (SQLException e) {
			Log.print("Error : SQL execution failed on preparedStatement.");
		}

		return null;
	}

    public void setPreparedStatement(String... queries)
    {
        _checkDBOpen();

        try
        {
            for (String query : queries)
            {
                _lstpplStmt.add(_conn.prepareStatement(query));
            }
        }
        catch (SQLException ex)
        {
            Log.print("Error : SQL execution failed on preparedStatement.");
        }

        _isStmtPrepared = true;
    }


    public void executeUpdatePrepared(Object[] bindArray)
    {
        _checkDBOpen();

        if (_isStmtPrepared)
        {
            throw new DaemonException("Error : SQL statement is not prepared. You must run setPreparedStatement() to prepare SQL statements first.", false);
        }

        try
        {
            for (int i = 0; i < _lstpplStmt.size(); i++)
            {
                PreparedStatement pstmt;
                int j;

                pstmt = _lstpplStmt.get(i);
                j = 0;

                for (Object bind : bindArray)
                {
                    pstmt.setString(j++ + 1, String.valueOf(bind));
                }

                Log.print(pstmt.toString());
                pstmt.executeUpdate();
            }
        }
        catch (SQLException ex)
        {
            throw new DaemonException(String.format("Error : SQL execution failed.\n%s", ex.toString()), false);
        }
    }

    public void setAutoCommit(boolean autoCommit)
    {
        _checkDBOpen();

        try
        {
            _conn.setAutoCommit(autoCommit);
        }
        catch (SQLException ex)
        {
            throw new DaemonException("Error : Unable to set auto commit for Oracle database.", false);
        }
    }

	public void commit() {
		commit(true);
	}

    public void commit(final boolean outputLog)
    {
        _checkDBOpen();

        try
        {
            _conn.commit();
        }
        catch (SQLException ex)
        {
            throw new DaemonException("Error : DBManager.commit() failed.", false);
        }

        if (outputLog) {
        	Log.print("INFO : DBManager.commit() complete.");
        }
    }

    public void rollback()
    {
        _checkDBOpen();

        try
        {
            _conn.rollback();
        }
        catch (SQLException ex)
        {
            Log.print(String.format("Error : DBManager.rollback -> %s", ex.toString()));
        }

        Log.print("INFO : DBManager.rollback() complete.");
    }

    public void safeClose()
    {
        try
        {
            if (_conn != null)
            {
                _conn.close();
            }
        }
        catch (Exception ignored)
        {
        }

        try
        {
            if (_stmt != null)
            {
                _stmt.close();
            }
        }
        catch (Exception ignored)
        {
        }

        try
        {
            for (int i = 0; i < _lstpplStmt.size(); i++)
            {
                if (_lstpplStmt.get(i) != null)
                {
                    _lstpplStmt.get(i).close();
                }
            }
        }
        catch (Exception ignored)
        {
        }

        try
        {
            if (_rs != null)
            {
                _rs.close();
            }
        }
        catch (Exception ignored)
        {
        }
    }

    private void _checkDBOpen()
    {
        if (!_isInitialized)
        {
            throw new DaemonException("Error : Tried to run SQL query while database is not open.");
        }
    }
    
    public Connection getConnection() {
    	return this._conn;
    }

    @Override
    public void close() throws IOException
    {
        safeClose();
    }
}