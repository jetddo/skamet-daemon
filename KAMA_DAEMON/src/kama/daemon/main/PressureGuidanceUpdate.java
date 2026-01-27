package kama.daemon.main;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class PressureGuidanceUpdate {
	public static void main(String[] args) {
		
		Date fctTm = null;
		System.out.println("args len:" + args.length);
		if(args.length == 1) {
			final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyyMMddHHmm");
			try {
				fctTm = fctTmFormat.parse(args[0]);
				System.out.println("set fct_tm:" + args[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		PressureGuidanceUpdate pressure = new PressureGuidanceUpdate(fctTm);
		pressure.run();
		/*
		try {
			pressure.repairAMOS_3H();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//*/
	}
	
	Date fctTm = null;
	Date startTm = null;
	Date endTm = null;
	final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyyMMddHHmm");
	
	public PressureGuidanceUpdate(Date fctTm) {
		
		StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("pwkey");
		
		try {
			Configurations configs = new Configurations();
			Configuration config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			urlAAMI = config.getString("db.url");
			idAAMI = config.getString("db.user");
			pwAAMI = encryptor.decrypt(config.getString("db.password"));
			
			urlAMIS = config.getString("db.amis.url");
			idAMIS = config.getString("db.amis.user");
			pwAMIS = encryptor.decrypt(config.getString("db.amis.password"));
		} catch(Exception err) {
			err.printStackTrace();
		}
				
		if(fctTm != null) {
			this.fctTm = fctTm;
		}
		
		Date[] dt = getFctTmDate();
		
		if(dt != null) {
			this.startTm = dt[0];
			this.endTm = dt[1];
			this.fctTm = dt[2];
		} else {
			System.out.println("FCT_TM IS NULL!!! Process Exit.");
		}
	}
		
	private String urlAAMI = "jdbc:oracle:thin:@192.168.0.128:1521:aami";
	private String idAAMI = "aami";
	private String pwAAMI = "koast3369";
	
	private String urlAMIS = "jdbc:oracle:thin:@192.168.0.128:1521:aami";
	private String idAMIS = "amis";
	private String pwAMIS = "koast3369";
	
	public void run() {
		if(this.fctTm == null) {
			return;
		}
		System.out.println("Pressure Update Start: " + new Date());
		updateAMOS_3H(); // 3시간 전 AMOS 수집
		updateAMOS_NOW(); // 현재 AMOS 수집
		updateMETAR_NOW(); // 현재 METAR 수집
		
		corrPRESSURE();	// Pressure 보정
		System.out.println("Pressure Update End: " + new Date());
	}
	
	private void updateQuery(boolean isAMIS, List<String> list) {
		Connection _conn = null;
		Statement _stmt = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			if(isAMIS) {
				_conn = DriverManager.getConnection(urlAMIS, idAMIS, pwAMIS);
			} else {
				_conn = DriverManager.getConnection(urlAAMI, idAAMI, pwAAMI);
			}
			_stmt = _conn.createStatement();
			
			for(int i = 0, sz=list.size(); i < sz; i++) {
				try {
					int rst = _stmt.executeUpdate(list.get(i));
					// System.out.println("query: " + list.get(i));
					// System.out.println("Result Code: " + rst);
				} catch (Exception e) {
					// System.out.println("query: " + list.get(i));
					e.printStackTrace();
				}
			}
			
			_stmt.close();
			_conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private List<Object[]> selectQuery(boolean isAMIS, String query) {
		Connection _conn = null;
		Statement _stmt = null;
		
		List<Object[]> list = new ArrayList<Object[]>();
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			if(isAMIS) {
				_conn = DriverManager.getConnection(urlAMIS, idAMIS, pwAMIS);
			} else {
				_conn = DriverManager.getConnection(urlAAMI, idAAMI, pwAAMI);
			}
			_stmt = _conn.createStatement();
			
			ResultSet rs = _stmt.executeQuery(query);
			ResultSetMetaData metaData = rs.getMetaData();
			int sizeOfColumn = metaData.getColumnCount();
			
			while(rs.next()) {
				Object[] item = new Object[sizeOfColumn];
				for(int i = 0; i < sizeOfColumn; i++) {
					item[i] = rs.getString(i+1);
				}
				list.add(item);
			};
			
			_stmt.close();
			_conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	private Date[] getFctTmDate() {
		String query = "SELECT TO_CHAR(MIN(TM)-3/24, 'yyyyMMddHH24mi') minTm, TO_CHAR(MAX(TM), 'yyyyMMddHH24mi') maxTm, TO_CHAR(MAX(FCT_TM), 'yyyyMMddHH24mi') fctTm FROM AMIS.FCT_PRESSURE ";
		if(fctTm == null) {
			query += "WHERE FCT_TM=(SELECT MAX(FCT_TM) FROM AMIS.FCT_PRESSURE) AND PRESSURE_3H IS NOT NULL ";
		} else {
			query += "WHERE FCT_TM=TO_DATE('" + fctTmFormat.format(fctTm) + "', 'yyyyMMddHH24mi') AND PRESSURE_3H IS NOT NULL ";
		}
		System.out.println("getFctTmDate: " + query);
		Connection _conn = null;
		Statement _stmt = null;

		Date[] info = null;

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");

			_conn = DriverManager.getConnection(urlAAMI, idAAMI, pwAAMI);
			_stmt = _conn.createStatement();
			
			ResultSet rs = _stmt.executeQuery(query);
			
			if(rs.next()) {
				if(rs.getString("fctTm") != null) {
					info = new Date[3];
					
					info[0] = fctTmFormat.parse(rs.getString("minTm"));
					info[1] = fctTmFormat.parse(rs.getString("maxTm"));
					info[2] = fctTmFormat.parse(rs.getString("fctTm"));
				}
			}
			
			_stmt.close();
			_conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		return info;
	}
	
	// QFF : 해면기압, QFE : 현지기압
	// 해면기압 사용 중
	private void updateAMOS_3H() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.HOUR_OF_DAY, -1);
		cal.set(Calendar.MINUTE, 55);
		cal.set(Calendar.SECOND, 00);
		cal.set(Calendar.MILLISECOND, 0);
		
		Date endTmpTm = new Date(endTm.getTime());
		
		// 종료 시간이 미래 시간인 경우 1시간 전 시간으로 변경
		if(endTm.getTime() > cal.getTime().getTime()) {
			endTmpTm = new Date(cal.getTime().getTime());
		}
		
		String query = "SELECT TO_CHAR(a.TM, 'yyyyMMddHH24mi') TM, TO_CHAR(TO_DATE(TO_CHAR(a.TM + 4/(24), 'yyyy-MM-dd HH24:\"00\"'), 'yyyy-MM-dd HH24:mi'), 'yyyyMMddHH24mi') FCT_TM, a.STN_ID, a.RWY_DIR, a.QFF/10 PRESSURE "
				+ "FROM ( "
				+ "SELECT amos.STN_ID, amos.RWY_DIR, MAX(amos.TM) TM "
				+ "FROM ( "
				+ "SELECT amos.STN_ID, amos.RWY_DIR, amos.TM, amos.QFF/10 PRESSURE FROM AMISUSER.AMOS amos LEFT OUTER JOIN AMISUSER.SETUP_OPTION opt ON amos.STN_ID=opt.STN_ID AND amos.RWY_DIR=opt.STD_DIR "
				+ "WHERE TM BETWEEN to_date('" + fctTmFormat.format(startTm) + "', 'yyyyMMddHH24mi') - 4/(24) AND to_date('" + fctTmFormat.format(endTmpTm) + "', 'yyyyMMddHH24mi') "
				+ "AND amos.RWY_DIR=opt.STD_DIR AND TO_CHAR(amos.TM, 'mi') < 56 "
				+ ") amos "
				+ "group by amos.STN_ID, amos.RWY_DIR, TO_CHAR(amos.TM, 'yyyy-MM-dd HH24') "
				+ ") b LEFT OUTER JOIN AMISUSER.AMOS a ON a.STN_ID = b.STN_ID AND a.RWY_DIR=b.RWY_DIR AND a.TM = b.TM ORDER BY a.TM";
		
		List<Object[]> list = selectQuery(true, query);
		System.out.println("updateAMOS_3H: " + query);
		if(list.size() > 0) {
			List<String> sql = new ArrayList<String>();
			
			for(int i = 0, sz=list.size(); i < sz; i++) {
				Object[] item = list.get(i);
				query = "UPDATE AMIS.FCT_PRESSURE SET "
						+ "TM_AMOS_3H=TO_DATE('" + item[0] + "', 'yyyyMMddHH24mi') "
						+ ", AMOS_3H=" + item[4]
						+ ", RWY_DIR='" + item[3] + "'"
						+ " WHERE FCT_TM BETWEEN TO_DATE('" + fctTmFormat.format(fctTm) + "', 'yyyyMMddHH24mi')- 12/24 AND TO_DATE('" + fctTmFormat.format(fctTm) + "', 'yyyyMMddHH24mi') "
						+ " AND TM>=TO_DATE('" + item[1] + "', 'yyyyMMddHH24mi') "
						// + " WHERE TM>=TO_DATE('" + item[1] + "', 'yyyyMMddHH24mi') "
						+ " AND STN_ID=" + item[2] + " ";
				// System.out.println(query);
				sql.add(query);
			}
			
			updateQuery(false, sql);
			
			query = "SELECT TO_CHAR(TM, 'yyyyMMddHH24mi') TM1, TO_CHAR((TM+3/24), 'yyyyMMddHH24mi') TM, TO_CHAR(FCT_TM, 'yyyyMMddHH24mi') FCT_TM, STN_ID, PRESSURE_NOW "
					+ "FROM AMIS.FCT_PRESSURE "
					+ "WHERE TM = TO_DATE('" + fctTmFormat.format(endTmpTm) + "', 'yyyyMMddHH24mi') - (175/60/24) + 3/24 "
					+ "ORDER BY FCT_TM, TM, STN_ID";
			
			List<Object[]> list_pressure_3h = selectQuery(false, query);
			System.out.println("updatePRESSURE_3H: " + query);
			List<String> sql_pressure_3h = new ArrayList<String>();
			
			for(int i = 0, sz=list_pressure_3h.size(); i < sz; i++) {
				Object[] item = list_pressure_3h.get(i);
				query = "UPDATE AMIS.FCT_PRESSURE SET "
						+ "PRESSURE_3H=" + item[4]
						+ " WHERE TM>=TO_DATE('" + item[1] + "', 'yyyyMMddHH24mi') "
						+ " AND FCT_TM=TO_DATE('" + item[2] + "', 'yyyyMMddHH24mi') "
						+ " AND STN_ID=" + item[3] + " ";
				System.out.println(query);
				sql_pressure_3h.add(query);
			}				
			updateQuery(false, sql_pressure_3h);
		}
	}
	
	private void updateAMOS_NOW() {
		String query = "SELECT TO_CHAR(amos.TM, 'yyyyMMddHH24mi') TM, amos.STN_ID, amos.RWY_DIR, amos.QFF/10 PRESSURE "
				+ "FROM AMISUSER.AMOS amos LEFT OUTER JOIN AMISUSER.SETUP_OPTION opt ON amos.STN_ID=opt.STN_ID AND amos.RWY_DIR=opt.STD_DIR "
				+ "WHERE TM BETWEEN to_date('" + fctTmFormat.format(startTm) + "', 'yyyyMMddHH24mi')-3/24 AND to_date('" + fctTmFormat.format(endTm) + "', 'yyyyMMddHH24mi') AND TO_CHAR(TM, 'mi')=00 AND amos.QFF > -99 "
				+ "AND amos.RWY_DIR=opt.STD_DIR ";
		
		List<Object[]> list = selectQuery(true, query);
		System.out.println("updateAMOS_NOW: " + query);
		if(list.size() > 0) {
			List<String> sql = new ArrayList<String>();
			
			for(int i = 0, sz=list.size(); i < sz; i++) {
				Object[] item = list.get(i);
				query = "UPDATE AMIS.FCT_PRESSURE SET "
						+ " AMOS_NOW=" + item[3]
						// + " WHERE FCT_TM=TO_DATE('" + fctTmFormat.format(fctTm) + "', 'yyyyMMddHH24mi') "
						// + " AND TM=TO_DATE('" + item[0] + "', 'yyyyMMddHH24mi') "
						+ " WHERE TM=TO_DATE('" + item[0] + "', 'yyyyMMddHH24mi') "
						+ " AND STN_ID=" + item[1] + " ";
				
				sql.add(query);
			}
			
			updateQuery(false, sql);
		}
	}
	
	private void updateMETAR_NOW() {
		String query = "SELECT TO_CHAR(metar.TM+9/24, 'yyyyMMddHH24mi') TM, opt.STN_ID, metar.QFF/10 PRESSURE "
				+ "FROM AMISUSER.METAR metar LEFT OUTER JOIN AMISUSER.SETUP_OPTION opt ON metar.STN_CD=opt.STN_CD "
				+ "WHERE TM BETWEEN to_date('" + fctTmFormat.format(startTm) + "', 'yyyyMMddHH24mi')-12/24 AND to_date('" + fctTmFormat.format(endTm) + "', 'yyyyMMddHH24mi')-9/24 AND TO_CHAR(TM, 'mi')=00 AND metar.QFF > -99 "
				+ " AND metar.MSG_TYPE IN ('METAR' , 'METARSCIAL') AND INP_TYPE=0 AND metar.QFF IS NOT NULL AND metar.STN_CD IS NOT NULL ";
		
		List<Object[]> list = selectQuery(true, query);
		System.out.println("updateMETAR_NOW: " + query);
		if(list.size() > 0) {
			List<String> sql = new ArrayList<String>();
			
			for(int i = 0, sz=list.size(); i < sz; i++) {
				Object[] item = list.get(i);
				query = "UPDATE AMIS.FCT_PRESSURE SET "
						+ " METAR_NOW=" + item[2]
						// + " WHERE FCT_TM=TO_DATE('" + fctTmFormat.format(fctTm) + "', 'yyyyMMddHH24mi') "
						// + " AND TM=TO_DATE('" + item[0] + "', 'yyyyMMddHH24mi') "
						+ " WHERE TM=TO_DATE('" + item[0] + "', 'yyyyMMddHH24mi') "
						+ " AND STN_ID=" + item[1] + " ";
				
				sql.add(query);
				// System.out.println("updateMETAR_NOW(" + i + "): " + query);
			}
			
			updateQuery(false, sql);
		}
	}
	
	@SuppressWarnings("serial")
	private void corrPRESSURE() {
		String selectMaxTmQuery = "SELECT TO_CHAR(pressure.TM, 'yyyyMMddHH24mi') TM, TO_CHAR(pressure.FCT_TM, 'yyyyMMddHH24mi') FCT_TM, pressure.STN_ID, pressure.PRESSURE_3H, pressure.TM_AMOS_3H, pressure.AMOS_3H "
				+ " FROM AMIS.FCT_PRESSURE pressure JOIN ( "
				+ "      SELECT FCT_TM, STN_ID, MAX(TM_AMOS_3H) TM_AMOS_3H "
				+ "      FROM AMIS.FCT_PRESSURE GROUP BY FCT_TM, STN_ID "
				+ " ) a ON pressure.FCT_TM = a.FCT_TM AND pressure.STN_ID=a.STN_ID AND pressure.TM_AMOS_3H = a.TM_AMOS_3H "
				+ " AND pressure.TM = TO_DATE(TO_CHAR(a.TM_AMOS_3H + 4/(24), 'yyyy-MM-dd HH24:\"00\"'), 'yyyy-MM-dd HH24:mi') AND a.FCT_TM > sysdate - 3 ";
		
		System.out.println("selectMaxTmQuery: " + selectMaxTmQuery);
		List<Object[]> list = selectQuery(false, selectMaxTmQuery);
		
		List<String> updateList = new ArrayList<String>();
		for(int i = 0, sz = list.size();i < sz; i++) {
			String updateQuery = "UPDATE AMIS.FCT_PRESSURE SET PRESSURE_3H = " + list.get(i)[3] 
					+ " WHERE FCT_TM=TO_DATE('" + list.get(i)[1] + "', 'yyyyMMddHH24mi') "
					+ " AND STN_ID=" + list.get(i)[2]
					+ " AND TM>TO_DATE('" + list.get(i)[0] + "', 'yyyyMMddHH24mi') ";
			
			System.out.println("updateQuery: " + updateQuery);
			updateList.add(updateQuery);
		}
		
		updateQuery(false, updateList);
		
		// update corr : (pressure_3h - amos_3h) / 2 
		// score2 = (Math.abs(Math.round(pressureCorr) - Math.round(metar)) <= 1 ? 100 : 0);
		final String query = "UPDATE AMIS.FCT_PRESSURE SET CORR = (PRESSURE_3H - AMOS_3H)/2, PRESSURE_CORR = PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2 "
				+ ", AMOS_COMP = CASE WHEN AMOS_NOW IS NULL THEN NULL WHEN ABS(ROUND((PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2), 0) - ROUND(AMOS_NOW, 0)) <= 1 THEN 100 ELSE 0 END "
				+ ", METAR_COMP = CASE WHEN METAR_NOW IS NULL THEN NULL WHEN ABS(ROUND((PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2),0) - ROUND(METAR_NOW, 0)) <= 1 THEN 100 ELSE 0 END "
				+ "WHERE TM BETWEEN TO_DATE('" + fctTmFormat.format(startTm) + "', 'yyyyMMddHH24MI') AND TO_DATE('" + fctTmFormat.format(endTm) + "', 'yyyyMMddHH24MI') AND pressure_3h IS NOT NULL AND amos_3h IS NOT NULL ";
		System.out.println("corrPressure: " + query);
		
		updateQuery(false, new ArrayList<String>(){
			{add(query);}
			});
	}
	
	// patch
	public void repairAMOS_3H() throws ParseException {
		Calendar cal = Calendar.getInstance();
		
		// Date endTmpTm = new Date(endTm.getTime());
		Date endTmpTm = fctTmFormat.parse("202012270900");
		Date lastTmpTm = fctTmFormat.parse("202012292100");
		
		cal.setTime(endTmpTm);
		System.out.println("START: " + (cal.getTime().getTime() < lastTmpTm.getTime()));
		final String query2 = "UPDATE AMIS.FCT_PRESSURE SET CORR = (PRESSURE_3H - AMOS_3H)/2, PRESSURE_CORR = PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2 "
				+ ", AMOS_COMP = CASE WHEN AMOS_NOW IS NULL THEN NULL WHEN ABS(ROUND((PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2), 0) - ROUND(AMOS_NOW, 0)) <= 1 THEN 100 ELSE 0 END "
				+ ", METAR_COMP = CASE WHEN METAR_NOW IS NULL THEN NULL WHEN ABS(ROUND((PRESSURE_NOW - (PRESSURE_3H - AMOS_3H)/2), 0) - ROUND(METAR_NOW, 0)) <= 1 THEN 100 ELSE 0 END "
				+ "WHERE TM BETWEEN TO_DATE('" + fctTmFormat.format(endTmpTm) + "', 'yyyyMMddHH24MI') AND TO_DATE('" + fctTmFormat.format(lastTmpTm) + "', 'yyyyMMddHH24MI') AND pressure_3h IS NOT NULL AND amos_3h IS NOT NULL ";
		// System.out.println("corrPressure: " + query2);
		
		updateQuery(false, new ArrayList<String>(){
			{add(query2);}
			});
		
	}
}
