package kama.daemon.common.util;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import kama.daemon.main.DaemonMain;

/**
 * @author jetddo
 */
public class DaemonUtils
{
    /**
     * 문자열이 숫자인지 확인하는 함수
     * @param amount 검증할 문자열
     * @return 숫자일 경우 true.
     */
    public static boolean isNumber(String amount)
    {
        try
        {
            Double.parseDouble(amount);
        }
        catch (NumberFormatException e)
        {
            return false;
        }

        return true;
    }

    /**
     * 날짜 정보를 받아 파싱하고 n초를 더하여 리턴
     * @param sdf
     * @param dateStr
     * @param seconds
     * @return
     */
    public static String addSeconds(SimpleDateFormat sdf, String dateStr, int seconds)
    {
        Calendar cal = null;

        try
        {
            cal = new GregorianCalendar(Locale.KOREA);
            Date d = sdf.parse((String) dateStr);
            cal.setTime(d);
            cal.add(Calendar.SECOND, seconds);
        }
        catch (ParseException pe)
        {
            return "";
        }

        return sdf.format(cal.getTime());
    }
    
    /**
	 * ResultSet 으로부터 자동으로 Map 에 매핑
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, Object> getResultSetData(ResultSet rs) throws SQLException {
		
		ResultSetMetaData metaData = rs.getMetaData();
		Map<String, Object> map = new HashMap<String, Object>();

	    for(int i = 1; i <= metaData.getColumnCount(); i++){
	    	
	        String columnName = metaData.getColumnLabel(i);	        
	        map.put(columnName, rs.getString(columnName));
	    }
	    
	    return map;
	}
	
	public static String toCamelcase(String str) {
		
		String camelcase = "";
		String[] tokens = str.toLowerCase().split("_");
		
		for(String token : tokens) {
			
			if(camelcase.length() == 0) {
				
				camelcase += token;
				
			} else {
				
				camelcase += (token.charAt(0)+"").toUpperCase() + token.substring(1);
			}
		}
		
		return camelcase;		
	}
	
	/**
	 * ResultSet 으로부터 자동으로 Map 에 매핑
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, Object> getCamelcaseResultSetData(ResultSet rs) {
		
		try {
		
			ResultSetMetaData metaData = rs.getMetaData();
			Map<String, Object> map = new HashMap<String, Object>();
	
		    for(int i = 1; i <= metaData.getColumnCount(); i++){
		    	
		        String columnName = metaData.getColumnLabel(i);	   
		        
		        map.put(DaemonUtils.toCamelcase(columnName), rs.getString(columnName));
		    }
		    
		    return map;
	    
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static boolean isWindow() {
		
		String osName = System.getProperty("os.name");

		if(osName.toLowerCase().contains("window")) {
			return true;
		} else {
			return false;
		}
	}
	
	public static String getConfigFilePath() {
		
		String confPath;
		File configFile;

		confPath = DaemonMain.class.getClassLoader().getResource("").getPath();

		configFile = new File(String.format("%s%s", confPath.replaceAll("bin","conf"), "config.properties"));

		if (!configFile.exists()) {
			// This configuration is for Intellij debug mode
			// Intellij 디버그 모드에서 파일 root 경로를 다르게 가져오기에 따로 설정.
			configFile = new File("conf/config.properties");

			if (!configFile.exists()) {
				throw new RuntimeException("Error : unable to locate configuration file.");
			}
		}

		return configFile.getAbsolutePath();
	}
}