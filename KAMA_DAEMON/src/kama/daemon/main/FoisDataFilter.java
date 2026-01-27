package kama.daemon.main;

import java.io.File;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class FoisDataFilter {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : FoisDataFilter.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	public void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		if(this.initialize()) {
			
			try {
				
				Calendar cal = new GregorianCalendar();
				
				Date currentDate = new Date();
				
				String startTimeStr = sdf.format(currentDate);
				String endTimeStr =   sdf.format(currentDate);
				
				System.out.println("-> StartDate: " + startTimeStr + ", endDate: " + endTimeStr);
				
				Date startDt = sdf.parse(startTimeStr);
				Date endDt = sdf.parse(endTimeStr);
				
				Date dt = new Date(startDt.getTime());
				
				while(dt.getTime() <= endDt.getTime()) {
					
					String st = sdf.format(dt);
					String ed = sdf.format(dt);
					
					System.out.println("\t-> Part Running [ " + st + " ~ " + ed + "]");
					
					List<Map<String, Object>> foisDataList = this.getFoisDataList(st, ed);
					
					System.out.println("\t-> foisDataList Size: " + foisDataList.size());
					
					this.filterFoisDataList(foisDataList);
					
					System.out.println("\t-> filtered foisDataList Size: " + foisDataList.size());
					
					this.insertFilteredFoisDataList(foisDataList);
					
					cal.setTime(dt);
					cal.add(Calendar.DAY_OF_MONTH, 1);
					dt = cal.getTime();					
				}
				
			} catch (Exception e) {
				
			}
		}
		
		this.destroy();
	}
	
	public void filterFoisDataList(List<Map<String, Object>> foisDataList) {
		
		Map<String, String> fltPkMap = new HashMap<String, String>();
		
		for(int i=0 ; i<foisDataList.size() ; i++) {
		
			Map<String, Object> foisData = foisDataList.get(i);
			
			String fltPk = (String)foisData.get("FLT_PK");
			
			if(fltPkMap.get(fltPk) == null) {
				fltPkMap.put(fltPk, "true");
			} else {
				foisDataList.remove(i--);
				continue;
			}
		}
	}
	
	public void insertFilteredFoisDataList(List<Map<String, Object>> foisDataList) {
		
		System.out.println("\t\t-> Insert filtered foisDataList Start");
			
		for(int i=0 ; i<foisDataList.size() ; i++) {
			
			Map<String, Object> foisData = foisDataList.get(i);
			
			String fltPk = (String)foisData.get("FLT_PK");
			
			if(this.isDuplicateRow(fltPk)) {
				
				System.out.println("\t\t\t-> Found Duplicate Row: " + fltPk);
				this.deleteFilteredFoisData(fltPk);		
			}
			
			try {
			
				String query = this.buildInsertQuery(foisData);
				
				this.dbManager.executeQuery(query);
				
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		this.dbManager.commit();
	}
	
	private boolean isDuplicateRow(String fltPk) {
		
		try {
			
			String query = "SELECT COUNT(*) AS CNT FROM AAMI.FILTERED_FOIS_LOCAL WHERE FLT_PK = '" + fltPk + "'";
			
			ResultSet resultSet = this.dbManager.executeQuery(query);
			
			if(resultSet.next()) {
				
				int cnt = resultSet.getInt("CNT");
				
				if(cnt > 0) {					
					return true;			
				}
			}
			
		} catch (Exception e) {
			return false;
		}
		
		return false;
	}
	
	private String filterFltDtString(String s) {
		
		if(s != null && s.length() != 12) {
			return null;
		}
		
		return s;
	}
	
	private String buildInsertQuery(Map<String, Object> foisData) {
		
		String tmStmp = (String)foisData.get("TM_STMP");
		String fltPk = (String)foisData.get("FLT_PK");
		String msgType = (String)foisData.get("MSG_TYPE");
		String fltCarr = (String)foisData.get("FLT_CARR");
		String fltNo = (String)foisData.get("FLT_NO");
		String fltId = (String)foisData.get("FLT_ID");
		String origArpt = (String)foisData.get("ORIG_ARPT");
		String origArptIcao = (String)foisData.get("ORIG_ARPT_ICAO");
		String origArptKor = (String)foisData.get("ORIG_ARPT_KOR");
		String schdDtDep = (String)foisData.get("SCHD_DT_DEP");
		schdDtDep = this.filterFltDtString(schdDtDep);		
		String actlDtDep = (String)foisData.get("ACTL_DT_DEP");
		actlDtDep = this.filterFltDtString(actlDtDep);
		
		String destArpt = (String)foisData.get("DEST_ARPT");
		String destArptIcao = (String)foisData.get("DEST_ARPT_ICAO");
		String destArptKor = (String)foisData.get("DEST_ARPT_KOR");
		String schdDtArr = (String)foisData.get("SCHD_DT_ARR");
		schdDtArr = this.filterFltDtString(schdDtArr);
		String actlDtArr = (String)foisData.get("ACTL_DT_ARR");
		actlDtArr = this.filterFltDtString(actlDtArr);		
		
		String depStat = (String)foisData.get("DEP_STAT");
		String depRsonCode = (String)foisData.get("DEP_RSON_CODE");
		String arrStat = (String)foisData.get("ARR_STAT");
		String arrRsonCode = (String)foisData.get("ARR_RSON_CODE");
		String estmDtDep = (String)foisData.get("ESTM_DT_DEP");
		estmDtDep = this.filterFltDtString(estmDtDep);
		String estmDtArr = (String)foisData.get("ESTM_DT_ARR");
		estmDtArr = this.filterFltDtString(estmDtArr);
		
		String query = "INSERT INTO AAMI.FILTERED_FOIS_LOCAL VALUES " +
		" (TO_TIMESTAMP(''{0}'', ''YYYYMMDDHH24MISS.FF3''),''{1}'',''{2}'',''{3}'',''{4}'',''{5}'',''{6}'',''{7}'',''{8}'',"+ 
		"TO_DATE(''{9}'', ''YYYYMMDDHH24MI''),TO_DATE(''{10}'', ''YYYYMMDDHH24MI''),''{11}'',''{12}'',''{13}'',"+ 
		"TO_DATE(''{14}'', ''YYYYMMDDHH24MI''),TO_DATE(''{15}'', ''YYYYMMDDHH24MI''),''{16}'',''{17}'',''{18}'',''{19}'', "+
		"TO_DATE(''{20}'', ''YYYYMMDDHH24MI''),TO_DATE(''{21}'', ''YYYYMMDDHH24MI''))";
		
		return MessageFormat.format(query, new Object[] {
				tmStmp, fltPk, msgType, fltCarr, fltNo, fltId, origArpt, origArptIcao, origArptKor, schdDtDep, actlDtDep, destArpt, destArptIcao,
				destArptKor, schdDtArr, actlDtArr, depStat, depRsonCode, arrStat, arrRsonCode, estmDtDep, estmDtArr
		}).replaceAll("'null'", "null");
	}
	
	public void deleteFilteredFoisData(String fltPk) {
		
		String query = "DELETE AAMI.FILTERED_FOIS_LOCAL WHERE FLT_PK = '"+fltPk+"'";
		
		this.dbManager.executeQuery(query);		
	}

	public List<Map<String, Object>> getFoisDataList(String stDtStr, String edDtStr) {
		
		String query = ""+
		
			"SELECT "+ 
			"	TO_CHAR(TM_STMP,'YYYYMMDDHH24MISS.FF3') AS TM_STMP, "+ 
			"	FLT_PK AS FLT_PK, "+ 
			"	A.MSG_TYPE AS MSG_TYPE, "+ 
			"	FLT_CARR AS FLT_CARR, "+ 
			"	FLT_NO AS FLT_NO, "+ 
			"	FLT_ID AS FLT_ID, "+ 
			"	ORIG_ARPT AS ORIG_ARPT, "+
			"	B.ICAO AS ORIG_ARPT_ICAO, "+
			"	B.NAME_KOR AS ORIG_ARPT_KOR, "+ 
			"	SCHD_DATE_DEP||SCHD_TIME_DEP AS SCHD_DT_DEP, "+
			"	ESTM_DATE_DEP||ESTM_TIME_DEP AS ESTM_DT_DEP, "+ 
			"	ACTL_DATE_DEP||ACTL_TIME_DEP AS ACTL_DT_DEP, "+ 
			"	DEST_ARPT AS DEST_ARPT, "+ 
			"	C.ICAO AS DEST_ARPT_ICAO, "+
			"	C.NAME_KOR AS DEST_ARPT_KOR, "+ 
			"	SCHD_DATE_ARR||SCHD_TIME_ARR AS SCHD_DT_ARR, "+
			"	ESTM_DATE_ARR||ESTM_TIME_ARR AS ESTM_DT_ARR, "+ 
			"	ACTL_DATE_ARR||ACTL_TIME_ARR AS ACTL_DT_ARR, "+ 
			"	DEP_STAT AS DEP_STAT, "+ 
			"	DEP_RSON_CODE AS DEP_RSON_CODE, "+ 
			"	ARR_STAT AS ARR_STAT, "+ 
			"	ARR_RSON_CODE AS ARR_RSON_CODE "+
			"FROM AAMI.FOIS A "+
			"LEFT OUTER JOIN AAMI.WORLD_AIRPORT_INFO B "+
			"ON A.ORIG_ARPT = B.IATA "+
			"LEFT OUTER JOIN AAMI.WORLD_AIRPORT_INFO C "+
			"ON A.DEST_ARPT = C.IATA "+
			"WHERE 1=1 ";	
		
		query += " AND (TO_DATE(SCHD_DATE_DEP, 'YYYYMMDD') >= TO_DATE('" + stDtStr + "', 'YYYYMMDD') AND TO_DATE(SCHD_DATE_DEP, 'YYYYMMDD') <= TO_DATE('" + edDtStr + "', 'YYYYMMDD')) ";
		query += " ORDER BY TM_STMP DESC";
		
		List<Map<String, Object>> foisDataList = new ArrayList<Map<String, Object>>();
		
		try {
			
			ResultSet resultSet = this.dbManager.executeQuery(query);
			
			while(resultSet.next()) {
				Map<String, Object> foisDataMap = DaemonUtils.getResultSetData(resultSet);	
				foisDataList.add(foisDataMap);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		return foisDataList;
	}
	
	
	public static void main(String[] args) {
		
		FoisDataFilter foisDataFilter = new FoisDataFilter();
		foisDataFilter.process();
	}
}
