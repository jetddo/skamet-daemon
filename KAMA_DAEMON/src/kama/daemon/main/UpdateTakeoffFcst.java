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

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class UpdateTakeoffFcst {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] stnIdArray = new String[]{"113", "092", "167", "110", "182", "163", "151", "153", "128", "142", "158", "139", "161", "986", "114"};
	private String[] rwyDirArray = new String[]{"15L", "33", "17", "14R", "07", "01", "36", "36", "24", "31", "04", "10", "06", "17", "03"};
		
	private final String getAmosDataQuery = 
			
			" SELECT TO_CHAR(TM-9/24, ''YYYYMMDDHH24MI'') AS TM, STN_ID, RWY_DIR, TMP/10 AS TMP, QNH/10*0.029525*100 AS QNH, WD_10MIN_AVG AS wd, ROUND(WSPD_10MIN_AVG/10, 1) AS ws FROM AMISUSER.AMOS "+
			" WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24'') + 9/24 - 4/24 AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24'') + 9/24 + 4/24 AND STN_ID = ''{2}'' AND RWY_DIR = ''{3}'' "+
			" ORDER BY TM ASC ";
	
	private final String getRecentKlapsIssuedTm = 
			
			" SELECT DISTINCT TO_CHAR(ISSUED_TM, 'YYYYMMDDHH24') AS ISSUED_TM FROM AMIS.FCT_KLAPS WHERE ISSUED_TM > SYSDATE - 1 ORDER BY ISSUED_TM ASC";
		
	private final String updateAmosDataQuery = 
			
			" UPDATE AMIS.FCT_KLAPS SET "+
			" AMOS_TMP = ''{0}'', AMOS_TMP_3H = ''{1}'', "+
			" AMOS_QNH = ''{2}'', AMOS_QNH_3H = ''{3}'', "+
			" AMOS_WD = ''{4}'', AMOS_WD_3H = ''{5}'', "+
			" AMOS_WS = ''{6}'', AMOS_WS_3H = ''{7}'', "+
			" AMOS_3H_TM = TO_DATE(''{8}'', ''YYYYMMDDHH24MI'') "+
			" WHERE STN_ID = ''{9}'' AND ISSUED_TM = TO_DATE(''{10}'', ''YYYYMMDDHH24'') AND FCST_TM = TO_DATE(''{11}'', ''YYYYMMDDHH24'') ";
	
	private final String getMosDataQuery = 
			
			" SELECT                       				 			"+
			" 	TO_CHAR(TM-9/24, 'YYYYMMDDHH24') AS FCST_TM,  		"+
			" 	TO_CHAR(FCT_TM, 'YYYYMMDDHH24') AS ISSUED_TM,       "+
			"   STN_ID,												"+
			" 	MOS_NOW AS MOS_TMP,         						"+
			" 	MOS_3H AS MOS_TMP_3H,       						"+
			" 	MOS_CORR AS MOS_TMP_CORR,   						"+
			" 	AMOS_COMP AS MOS_TMP_SCORE, 						"+
			" 	CORR AS MOS_TMP_CORR_CF     						"+
			" FROM AMIS.FCT_MOS             						"+
			" WHERE 1=1				        						"+
			" AND TM >= SYSDATE - 15/24     						"+
			" ORDER BY TM ASC, FCT_TM ASC   						";
	
	private final String updateMosDataQuery = 
			
			" UPDATE AMIS.FCT_KLAPS SET                                                  "+
			" MOS_TMP = ''{2}'',                                                         "+
			" MOS_TMP_3H = ''{3}'',                                                      "+
			" MOS_TMP_CORR = ''{4}'',                                                    "+
			" MOS_TMP_CORR_CF = ''{5}'',                                                 "+
			" MOS_TMP_SCORE = ''{6}''                                                    "+
			" WHERE STN_ID = {0} AND FCST_TM = TO_DATE(''{1}'', ''YYYYMMDDHH24'')	     ";
		
	private final String calculateKlapsCorrQuery = 
			
			"UPDATE AMIS.FCT_KLAPS SET "+
			" TMP_CORR = KLAPS_TMP - (KLAPS_TMP_3H - AMOS_TMP_3H)/2, TMP_CORR_CF = (KLAPS_TMP_3H - AMOS_TMP_3H)/2, "+
			" QNH_CORR = KLAPS_QNH - (KLAPS_QNH_3H - AMOS_QNH_3H)/2, QNH_CORR_CF = (KLAPS_QNH_3H - AMOS_QNH_3H)/2, "+
			" WD_CORR = KLAPS_WD - (KLAPS_WD_3H - AMOS_WD_3H)/2, WD_CORR_CF = (KLAPS_WD_3H - AMOS_WD_3H)/2,  "+
			" WS_CORR = KLAPS_WS - (KLAPS_WS_3H - AMOS_WS_3H)/2, WS_CORR_CF = (KLAPS_WS_3H - AMOS_WS_3H)/2 "+
			"WHERE ISSUED_TM = TO_DATE(''{0}'', ''YYYYMMDDHH24'') " +
			"AND KLAPS_TMP_3H IS NOT NULL AND AMOS_TMP_3H IS NOT NULL "+
			"AND KLAPS_QNH_3H IS NOT NULL AND AMOS_QNH_3H IS NOT NULL "+
			"AND KLAPS_WD_3H IS NOT NULL AND AMOS_WD_3H IS NOT NULL "+
			"AND KLAPS_WS_3H IS NOT NULL AND AMOS_WS_3H IS NOT NULL ";
	
	private final String getKlapsDatabyIssuedTm = 
			
			"SELECT TO_CHAR(ISSUED_TM, ''YYYYMMDDHH24'') AS ISSUED_TM, TO_CHAR(FCST_TM, ''YYYYMMDDHH24'') AS FCST_TM, STN_ID, AMOS_TMP, AMOS_QNH, AMOS_WD, AMOS_WS, TMP_CORR, QNH_CORR, WD_CORR, WS_CORR FROM AMIS.FCT_KLAPS WHERE ISSUED_TM = TO_DATE(''{0}'', ''YYYYMMDDHH24'') ";
	
	private final String calculateKlapsScoreQuery = 
			
			"UPDATE AMIS.FCT_KLAPS SET "+
			" TMP_SCORE = ''{0}'', "+
			" QNH_SCORE = ''{1}'', "+
			" WD_SCORE = ''{2}'', "+
			" WS_SCORE = ''{3}'' "+			
			"WHERE STN_ID = ''{4}'' " +
			"AND ISSUED_TM = TO_DATE(''{5}'', ''YYYYMMDDHH24'') " +
			"AND FCST_TM = TO_DATE(''{6}'', ''YYYYMMDDHH24'') ";
	
	private final String deleteKlapsDataListForAmisDB = 
			
			"DELETE FLY.FCT_KLAPS WHERE ISSUED_TM > SYSDATE-{0}";
	
	private final String getKlapsDataListForAmisDB = 
			
			"SELECT                                                     "+
			" TO_CHAR(ISSUED_TM, ''YYYYMMDDHH24MISS'') AS ISSUED_TM     "+
			",TO_CHAR(FCST_TM, ''YYYYMMDDHH24MISS'') AS FCST_TM         "+
			",STN_ID                                                    "+
			",KLAPS_TMP                                                 "+
			",KLAPS_TMP_3H                                              "+
			",KLAPS_QNH                                                 "+
			",KLAPS_QNH_3H                                              "+
			",KLAPS_WD                                                  "+
			",KLAPS_WD_3H                                               "+
			",KLAPS_WS                                                  "+
			",KLAPS_WS_3H                                               "+
			",AMOS_TMP                                                  "+
			",AMOS_TMP_3H                                               "+
			",AMOS_QNH                                                  "+
			",AMOS_QNH_3H                                               "+
			",AMOS_WD                                                   "+
			",AMOS_WD_3H                                                "+
			",AMOS_WS                                                   "+
			",AMOS_WS_3H                                                "+
			",TMP_CORR                                                  "+
			",QNH_CORR                                                  "+
			",WD_CORR                                                   "+
			",WS_CORR                                                   "+
			",TO_CHAR(AMOS_3H_TM, ''YYYYMMDDHH24MISS'') AS AMOS_3H_TM   "+
			",TMP_CORR_CF                                               "+
			",QNH_CORR_CF                                               "+
			",WD_CORR_CF                                                "+
			",WS_CORR_CF                                                "+
			",TMP_SCORE                                                 "+
			",QNH_SCORE                                                 "+
			",WD_SCORE                                                  "+
			",WS_SCORE                                                  "+
			",MOS_TMP                                             	    "+
			",MOS_TMP_3H                                                "+
			",MOS_TMP_CORR                                              "+
			",MOS_TMP_CORR_CF                                           "+
			",MOS_TMP_SCORE                                             "+
			",KLAPS_QNH_HPA                                             "+
			"FROM AMIS.FCT_KLAPS                                        "+
			"WHERE ISSUED_TM > SYSDATE-{0}                              ";
	
	private final String insertKlapsDataListForAmisDB =
			
			"INSERT INTO FLY.FCT_KLAPS(ISSUED_TM,FCST_TM,STN_ID,KLAPS_TMP,KLAPS_TMP_3H,KLAPS_QNH,KLAPS_QNH_3H,KLAPS_WD,KLAPS_WD_3H,KLAPS_WS,KLAPS_WS_3H,AMOS_TMP,AMOS_TMP_3H,AMOS_QNH,AMOS_QNH_3H,AMOS_WD,AMOS_WD_3H,AMOS_WS,AMOS_WS_3H,TMP_CORR,QNH_CORR,WD_CORR,WS_CORR,AMOS_3H_TM,TMP_CORR_CF,QNH_CORR_CF,WD_CORR_CF,WS_CORR_CF,TMP_SCORE,QNH_SCORE,WD_SCORE,WS_SCORE,MOS_TMP,MOS_TMP_3H,MOS_TMP_CORR,MOS_TMP_CORR_CF,MOS_TMP_SCORE,KLAPS_QNH_HPA) "+
			" VALUES (TO_DATE(''{0}'', ''YYYYMMDDHH24MISS''),TO_DATE(''{1}'', ''YYYYMMDDHH24MISS''),''{2}'',''{3}'',''{4}'',''{5}'',''{6}'',''{7}'',''{8}'',''{9}'',''{10}'',''{11}'',''{12}'',''{13}'',''{14}'',''{15}'',''{16}'',''{17}'',''{18}'',''{19}'',''{20}'',''{21}'',''{22}'',TO_DATE(''{23}'', ''YYYYMMDDHH24MISS''),''{24}'',''{25}'',''{26}'',''{27}'',''{28}'',''{29}'',''{30}'',''{31}'',''{32}'',''{33}'',''{34}'',''{35}'',''{36}'',''{37}'')";	
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private AmisDataBaseManager amisDbmanager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			this.amisDbmanager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateTakeoffFcst.initialize -> " + e);
			
			this.dbManager.safeClose();
			this.amisDbmanager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
		this.amisDbmanager.safeClose();
	}
	
	private List<Map<String, Object>> getKlapsDatabyIssuedTm(String issuedTm) {
		
		List<Map<String, Object>> klapsDataList = new ArrayList<Map<String, Object>>();
		
		ResultSet resultSet = dbManager.executeQuery(MessageFormat.format(this.getKlapsDatabyIssuedTm, new Object[]{
				issuedTm
		}));
		
		try {
			
			while(resultSet.next()) {
				
				klapsDataList.add(DaemonUtils.getCamelcaseResultSetData(resultSet));
			}
			
		} catch (Exception e) {
			
		}
		
		return klapsDataList;
	}
	
	private void updateMosData() {
		
		System.out.println("INFO : Update Mos Data");
				
		// MOS 기온값을 업데이트하자
		List<Map<String, Object>> mosDataList = getMosDataQueryList();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		for(int i=0 ; i<mosDataList.size() ; i++) {
			
			try {
				
				Map<String, Object> mosData = mosDataList.get(i);
				
				String issuedTmStr = (String)mosData.get("issuedTm");
				String fcstTmStr = (String)mosData.get("fcstTm");
				
				// 9시간 이상 차이가 날때부터 입력한다.
				
				if(Math.abs(sdf.parse(issuedTmStr).getTime() - sdf.parse(fcstTmStr).getTime()) < 1000 * 60 * 60 * 9) {
					continue;
				}
				
				String stnId = (String)mosData.get("stnId");
				String mosTmp3h = (String)mosData.get("mosTmp3h");
				String mosTmpCorr = (String)mosData.get("mosTmpCorr");
				String mosTmpCorrCf = (String)mosData.get("mosTmpCorrCf");
				String mosTmp = (String)mosData.get("mosTmp");
				String mosTmpScore = (String)mosData.get("mosTmpScore");
				
				String updateQuery = MessageFormat.format(this.updateMosDataQuery, new Object[]{
						stnId, fcstTmStr, mosTmp, mosTmp3h, mosTmpCorr, mosTmpCorrCf, mosTmpScore
				}).replace("'null'", "null");
				
				this.dbManager.executeUpdate(updateQuery);
				
			} catch (Exception e) {
				System.out.println("ERROR : Update Mos Data -> " + e.getMessage());
			}
		}
	}
	
	private List<Map<String, Object>> getMosDataQueryList() {
		
		List<Map<String, Object>> mosDataList = new ArrayList<Map<String, Object>>();
		
		ResultSet resultSet = dbManager.executeQuery(this.getMosDataQuery);
		
		try {
			
			while(resultSet.next()) {
				
				mosDataList.add(DaemonUtils.getCamelcaseResultSetData(resultSet));
			}
			
		} catch (Exception e) {
			
		}
		
		return mosDataList;
	}
	
	private List<Map<String, Object>> getRecentKlapsIssuedTmList() {
		
		
		List<Map<String, Object>> recentKlapsIssuedTmList = new ArrayList<Map<String, Object>>();
		
		ResultSet resultSet = dbManager.executeQuery(this.getRecentKlapsIssuedTm);
		
		try {
			
			while(resultSet.next()) {
				
				recentKlapsIssuedTmList.add(DaemonUtils.getCamelcaseResultSetData(resultSet));
			}
			
		} catch (Exception e) {
			
		}
		
		return recentKlapsIssuedTmList;
	}
	
	private boolean updateAmosData(String klapsIssuedTm) {
		
		// KLAPS 는 12시간 예측이당
		System.out.println("INFO : Update Amos Data [klapsIssuedTm=" + klapsIssuedTm + "]");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmm");
		
		Calendar cal = new GregorianCalendar();
		
		try {
			
			cal.setTime(sdf.parse(klapsIssuedTm));
			String stFcstTm = sdf.format(cal.getTime());
			
			cal.add(Calendar.HOUR_OF_DAY, 12);
			String edFcstTm = sdf.format(cal.getTime());
			
			for(int i=0 ; i<stnIdArray.length ; i++) {
				
				String stnId = stnIdArray[i];
				
				String rwyDir = rwyDirArray[i];
				
				List<Map<String, Object>> amosDataList = this.amisDbmanager.select(MessageFormat.format(this.getAmosDataQuery, new Object[]{					
					stFcstTm, edFcstTm, stnId, rwyDir						
				}));
				
				cal.setTime(sdf.parse(klapsIssuedTm));
				
				// KLAPS 는 13시간이므로 복잡하게 하지 말고 그냥 13번 찾는다
				
				Map<String, String> lastAmos3HData = null;
				
				for(int j=0 ; j<=12 ; j++) {
					
					Map<String, String> amosUpdateData = new HashMap<String, String>();
					
					amosUpdateData.put("stnId", stnId);
					amosUpdateData.put("issuedTm", klapsIssuedTm);
					
					String klapsFcstTm = sdf.format(cal.getTime());
					
					amosUpdateData.put("fcstTm", klapsFcstTm);
					
					long klapsFcstTick = sdf.parse(klapsFcstTm).getTime();
					
					// 현재꺼
					boolean isFind0H = false;
					
					// 3시간전꺼
					boolean isFind3H = false;
					
					for(int k=0 ; k<amosDataList.size() ; k++) {
					
						Map<String, Object> amosData = amosDataList.get(k);
						
						String amosTm = amosData.get("tm").toString();
						
						long amosTick = sdf2.parse(amosTm).getTime();
						
						if(!isFind0H && Math.abs(klapsFcstTick - amosTick) <= 5 * 1000 * 60) {
							
							isFind0H = true;
							
							Map<String, String> _amosData = this.getTakeoffElementFromAmosData(amosData);
							
							amosUpdateData.put("amosTmp", _amosData.get("tmp"));
							amosUpdateData.put("amosQnh", _amosData.get("qnh"));
							amosUpdateData.put("amosWd", _amosData.get("wd"));
							amosUpdateData.put("amosWs", _amosData.get("ws"));
						}
						
						if(!isFind3H && 
							(klapsFcstTick - amosTick <= 3 * 1000 * 60 * 60 + 5 * 1000 * 60) &&
							(klapsFcstTick - amosTick >= 3 * 1000 * 60 * 60 - 5 * 1000 * 60)){
							
							isFind3H = true;
							
							Map<String, String> _amosData = this.getTakeoffElementFromAmosData(amosData);
							
							amosUpdateData.put("amos3HTm", _amosData.get("tm"));
							amosUpdateData.put("amosTmp3H", _amosData.get("tmp"));
							amosUpdateData.put("amosQnh3H", _amosData.get("qnh"));
							amosUpdateData.put("amosWd3H", _amosData.get("wd"));
							amosUpdateData.put("amosWs3H", _amosData.get("ws"));
							
							lastAmos3HData = _amosData;
						}
					}
					
//					if(!isFind3H) {
//						
//						amosUpdateData.put("amos3HTm", lastAmos3HData.get("tm").toString());
//						amosUpdateData.put("amosTmp3H", lastAmos3HData.get("tmp"));
//						amosUpdateData.put("amosQnh3H", lastAmos3HData.get("qnh"));
//						amosUpdateData.put("amosWd3H", lastAmos3HData.get("wd"));
//						amosUpdateData.put("amosWs3H", lastAmos3HData.get("ws"));		
//					}
					
					dbManager.executeUpdate(					
						MessageFormat.format(this.updateAmosDataQuery, new Object[]{
							amosUpdateData.get("amosTmp"),
							amosUpdateData.get("amosTmp3H"),
							amosUpdateData.get("amosQnh"),
							amosUpdateData.get("amosQnh3H"),
							amosUpdateData.get("amosWd"),
							amosUpdateData.get("amosWd3H"),
							amosUpdateData.get("amosWs"),
							amosUpdateData.get("amosWs3H"),
							amosUpdateData.get("amos3HTm"),
							amosUpdateData.get("stnId"),
							amosUpdateData.get("issuedTm"),
							amosUpdateData.get("fcstTm"),
					}).replaceAll("'null'", "null"));
							
					cal.add(Calendar.HOUR_OF_DAY, 1);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private Map<String, String> getTakeoffElementFromAmosData(Map<String, Object> amosData) {
		
		Map<String, String> _amosData = new HashMap<String, String>();
		
		_amosData.put("tm", amosData.get("tm").toString());
		
		Object amosTmp = amosData.get("tmp");
		Object amosQnh = amosData.get("qnh");
		Object amosWd = amosData.get("wd");
		Object amosWs = amosData.get("ws");
		
		if(amosTmp != null) {
			_amosData.put("tmp", amosTmp.toString());
		} else {
			_amosData.put("tmp", "");
		}
		
		if(amosQnh != null) {
			_amosData.put("qnh", amosQnh.toString());
		} else {
			_amosData.put("qnh", "");
		}
		
		if(amosWd != null) {
			_amosData.put("wd", amosWd.toString());
		} else {
			_amosData.put("wd", "");
		}
		
		if(amosWs != null) {
			_amosData.put("ws", amosWs.toString());
		} else {
			_amosData.put("ws", "");
		}
		
		return _amosData;
	}
	
	private void calculateKlapsCorr(String klapsIssuedTm) {
		
		System.out.println("INFO : Calculate Corr [klapsIssuedTm=" + klapsIssuedTm + "]");
		
		dbManager.executeUpdate(MessageFormat.format(this.calculateKlapsCorrQuery, new Object[]{
			klapsIssuedTm
		}));
	}
	
	private void process() {
				
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start :::::");
		
		if(!this.initialize()) {
			
			System.out.println("Error : UpdateTakeoffFcst.process -> initialize failed");
			return;
		}
		
		updateMosData();
		
		List<Map<String, Object>> recentKlapsIssuedTmList = getRecentKlapsIssuedTmList();
		
//		if(recentKlapsIssuedTmList.size() == 0) {
//			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End :::::");
//			return;
//		}
		
		for(int i=0 ; i<recentKlapsIssuedTmList.size() ; i++) {
			
			String klapsIssuedTm = recentKlapsIssuedTmList.get(i).get("issuedTm").toString();
				
			if(this.updateAmosData(klapsIssuedTm)) {
				
				// 여기서 보정값을 계산한당
				this.calculateKlapsCorr(klapsIssuedTm);
			}
			
			// 점수를 계산한당
			this.updateKlapCorrScore(klapsIssuedTm);
		}
		
		this.dbManager.commit();
		
		// AMIS DB 동기화 부분
		
		this.synchronizeAmisDB(1);
		
		this.amisDbmanager.commit();
		
		this.destroy();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End :::::");
	}
	
	private void synchronizeAmisDB(int syncDays) {
		
		try {
			
			System.out.print("Start Synchronize Amis DB... ");
			
			this.amisDbmanager.delete(MessageFormat.format(this.deleteKlapsDataListForAmisDB, new Object[]{
				syncDays
			}));
						
			ResultSet resultSet = this.dbManager.executeQuery(MessageFormat.format(this.getKlapsDataListForAmisDB, new Object[]{
				syncDays
			}));
			
			List<Object[]> paramList = new ArrayList<Object[]>();
			
			while(resultSet.next()) {
			
				List<Object> objList = new ArrayList<Object>();
				
				for(int i=0 ; i<38 ; i++) {
					objList.add(resultSet.getString(i+1));	
				}
				
				this.amisDbmanager.insert(MessageFormat.format(this.insertKlapsDataListForAmisDB, objList.toArray()).replaceAll("'null'", "null"));
			}
			
			System.out.println("End");
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	private void updateKlapCorrScore(String klapsIssuedTm) {
		
		System.out.println("INFO : Update Klaps score [klapsIssuedTm=" + klapsIssuedTm + "]");
		
		// 점수 계산을 위해 데이터를 가져온당
		List<Map<String, Object>> klapsDataList = this.getKlapsDatabyIssuedTm(klapsIssuedTm);
		
		for(int i=0 ; i<klapsDataList.size() ; i++) {
			
			Map<String, Object> klapsData = klapsDataList.get(i);
			
			Float tmpCorr = klapsData.get("tmpCorr") != null ? Float.valueOf(klapsData.get("tmpCorr").toString()) : null;
			Float qnhCorr = klapsData.get("qnhCorr") != null ? Float.valueOf(klapsData.get("qnhCorr").toString()) : null;
			Float wdCorr = klapsData.get("wdCorr") != null ? Float.valueOf(klapsData.get("wdCorr").toString()) : null;
			Float wsCorr = klapsData.get("wsCorr") != null ? Float.valueOf(klapsData.get("wsCorr").toString()) : null;
			
			Float amosTmp = klapsData.get("amosTmp") != null ? Float.valueOf(klapsData.get("amosTmp").toString()) : null;
			Float amosQnh = klapsData.get("amosQnh") != null ? Float.valueOf(klapsData.get("amosQnh").toString()) : null;
			Float amosWd = klapsData.get("amosWd") != null ? Float.valueOf(klapsData.get("amosWd").toString()) : null;
			Float amosWs = klapsData.get("amosWs") != null ? Float.valueOf(klapsData.get("amosWs").toString()) : null;
			
			Float tmpScore = this.getScore("TMP", tmpCorr, amosTmp);
			Float qnhScore = this.getScore("QNH", qnhCorr, amosQnh);
			Float wdScore = this.getScore("WD", wdCorr, amosWd);
			Float wsScore = this.getScore("WS", wsCorr, amosWs);
					
			String stnId = klapsData.get("stnId").toString();
			String issuedTm = klapsData.get("issuedTm").toString();
			String fcstTm = klapsData.get("fcstTm").toString();
			
			dbManager.executeUpdate(MessageFormat.format(this.calculateKlapsScoreQuery,  new Object[]{
				
				tmpScore,
				qnhScore,
				wdScore,
				wsScore,
				stnId,
				issuedTm,
				fcstTm
					
			}).replaceAll("'null'", "null"));
		}
	}
	
	private Float getScore(String elementType, Float corrValue, Float obsValue) {
		
		if(corrValue == null || obsValue == null) {
			return null;
		}
		
		double gap = Math.abs(corrValue - obsValue);
		
		switch(elementType) {
		
		case "TMP": 
			
			if(gap <= 1) {
				return 100f;
			} else {
				return 0f;
			}
			
		case "QNH":
			
			gap = (gap / 0.029525 / 100);
			
			if(gap < 1.02) {
				return 100f;
			} else {
				return 0f;
			}
			
		case "WD": 
			
			gap = Math.min(gap, 360 - gap);
			
			if(gap <= 20) {	
				return 100f;
			} else if(gap > 20 && gap <= 50) {					
				return 50f;
			} else {						
				return 0f;
			}
			
		case "WS":
			
			if(gap <= 5) {
				return 100f;
			} else {
				return 0f;
			}	
		}
		
		return null;
	}
	
	public static void main(String[] args) {

		new UpdateTakeoffFcst().process();
	}

}