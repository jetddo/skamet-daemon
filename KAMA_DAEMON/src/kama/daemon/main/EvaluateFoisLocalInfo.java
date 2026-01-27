package kama.daemon.main;
import java.io.File;
import java.sql.ResultSet;
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

public class EvaluateFoisLocalInfo {
	
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
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		if(this.initialize()) {
			
			Calendar cal = new GregorianCalendar();
		
			Date currentDate = new Date();
			
			cal.setTime(currentDate);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			
			Date endTime = cal.getTime();
			
			cal.add(Calendar.DAY_OF_MONTH, -1);
			
			Date startTime = cal.getTime();
			
			String startTimeStr = sdf.format(startTime);
			String endTimeStr =   sdf.format(endTime);
			
			System.out.println("-> StartDate: " + startTimeStr + ", endDate: " + endTimeStr);
			
			this.evaluate(startTimeStr, endTimeStr);
		}
		
		this.destroy();
	}

	public void evaluate(String startTimeStr, String endTimeStr) {
	
		try {
				
			List<Map<String, Object>> foisLocalDataList = getFoisLocalInfoDataList(startTimeStr, endTimeStr);
				
			for (int i=0; i<foisLocalDataList.size() ; i++) {

				Map<String, Object> foisLocalData = foisLocalDataList.get(i);

				String fltPk = (String) foisLocalData.get("FLT_PK");
				String fltCarr = (String) foisLocalData.get("FLT_CARR");
				String fltCarrKor = getAirlineKorbyIATA(fltCarr);
				String fltId = (String) foisLocalData.get("FLT_ID");
				String schdDtDepStr = (String) foisLocalData.get("SCHD_DT_DEP");
				String actlDtDepStr = (String) foisLocalData.get("ACTL_DT_DEP");
				String estmDtDepStr = (String) foisLocalData.get("ESTM_DT_DEP");
				
//				schdDtDepStr = this.convertUTCtoKST(schdDtDepStr);
//				actlDtDepStr = this.convertUTCtoKST(actlDtDepStr);
//				estmDtDepStr = this.convertUTCtoKST(estmDtDepStr);
				
				String schdDtArrStr = (String) foisLocalData.get("SCHD_DT_ARR");
				String actlDtArrStr = (String) foisLocalData.get("ACTL_DT_ARR");
				String estmDtArrStr = (String) foisLocalData.get("ESTM_DT_ARR");
				
//				schdDtArrStr = this.convertUTCtoKST(schdDtArrStr);
//				actlDtArrStr = this.convertUTCtoKST(actlDtArrStr);
//				actlDtArrStr = this.convertUTCtoKST(actlDtArrStr);
				
				String origArpt = (String) foisLocalData.get("ORIG_ARPT");
				String origArptIcao = (String) foisLocalData.get("ORIG_ARPT_ICAO");
				String origArptKor = (String) foisLocalData.get("ORIG_ARPT_KOR");
				String destArpt = (String) foisLocalData.get("DEST_ARPT");
				String destArptIcao = (String) foisLocalData.get("DEST_ARPT_ICAO");
				String destArptKor = (String) foisLocalData.get("DEST_ARPT_KOR");
				
				String depStat = (String) foisLocalData.get("DEP_STAT");				
				String arrStat = (String) foisLocalData.get("ARR_STAT");
				
				String depStatCode = this.convertStatCode(depStat);
				String arrStatCode = this.convertStatCode(arrStat);
				
				String statCode = "0";
				
				if("0".equals(depStatCode) && !"0".equals(arrStatCode)) {
					statCode = arrStatCode;
				} else if(!"0".equals(depStatCode) && "0".equals(arrStatCode)) {
					statCode = depStatCode;
				} else {
					statCode = depStatCode;
				}
				
				String depRsonCode = (String) foisLocalData.get("DEP_RSON_CODE");
				String arrRsonCode = (String) foisLocalData.get("ARR_RSON_CODE");
				
				String depCause = this.convertRsonCodeKor(depRsonCode);
				String arrCause = this.convertRsonCodeKor(arrRsonCode);
				
				Integer depCauseCode = this.determineCauseCodes(depCause);
				Integer arrCauseCode = this.determineCauseCodes(arrCause);
				
				String cause = "";
				Integer causeCode = 0;
				
				if(depCauseCode == 0 && arrCauseCode == 0) {
					continue;
				} else if(depCauseCode == 0) {
					cause = arrCause;
					causeCode = arrCauseCode;
				} else if(arrCauseCode == 0) {
					cause = depCause;
					causeCode = depCauseCode;
				} else {
					cause = depCause;
					causeCode = depCauseCode;
				}
				
				String causeAirport = null; // DEP: 출발, ARR: 도착
				
				int evalResult = -1;
				int depEvalResult = -1;
				int arrEvalResult = -1;
			
				if(cause.contains("출발")) {
					causeAirport = "DEP";					
				} else if (cause.contains("목적")) {
					causeAirport = "ARR";
				}
				
				Map<String, Object> depWeatherData = null;
				Map<String, Object> arrWeatherData = null;
				
				/*
				 * 평가 결과 코드 구분: evalResult				 
				 *  -1: 무효 (출발, 도착 공항 모두 평가 무효인 경우) 
				 *  0: 불발 (평가 유효인 출발, 도착 둘중에 하나이상 불발)
				 *  1: 적중 (평가 유효인 출발, 도착 둘다 적중, 둘중 하나가 무효라면 하나만 적중해도 적중으로 판단)
				 *  2: 평가 대상 공항이 아닌 경우
				 */
				
				/*
				 * 평가 결과 코드 구분: (dep|arr)EvalResult				 
				 *  -1: 무효 (Metar 가 없거나 판단 대상이 아닌 경우 예: 실링) 
				 *  0: 불발 (Metar 중에 적중에 한게 하나도 없는 경우)
				 *  1: 적중 (Metar 중에 적중한게 1개라도 있는 경우)
				 */
				
				System.out.println("출발공항 : " + origArptKor + ", 도착공항 : " + destArptKor + ",  항공사 : " + fltCarrKor + ", 편명 : " + fltId + ", 원인 : " + cause + ", 원인코드 : " + causeCode + ", 출발시간 : " + schdDtDepStr + ", 도착시간 : " + schdDtArrStr);
							
				if (origArptIcao == null && destArptIcao == null) {
					
					System.out.println("-> 출발공항, 도착공항 둘 모두 평가 대상 공항이 아닌 경우");
					
					depEvalResult = 2;
					arrEvalResult = 2;
					
				} else {
					
					// 출발공항이 평가 대상이 아닌 경우 도착공항만 평가한다
					if(origArptIcao == null) {
						
						depEvalResult = 2;
						
						// 도착원인에 대한 기상정보
						arrWeatherData = this.getArrWeatherData(destArptIcao, schdDtDepStr, schdDtArrStr);
						
						// 도착공항 기상정보가 없는 경우 무효
						if(arrWeatherData.get("metarDataList") == null) {
							
							arrEvalResult = -1;
							
						} else {
							// 도착원인에 대한 평가
							arrEvalResult = this.evaluate(destArptIcao, arrWeatherData, causeCode);							
						}						
						
					// 도착공항이 평가 대상이 아닌 경우 출발공항만 평가한다
					} else if(destArptIcao == null) {
						
						arrEvalResult = 2;
						
						// 출발원인에 대한 기상정보
						depWeatherData = this.getDepWeatherData(origArptIcao, schdDtDepStr);
						
						// 출발공항 기상정보가 없는 경우 무효
						if (depWeatherData.get("metarDataList") == null) {

							depEvalResult = -1;

						} else {
							// 출발원인에 대한 평가
							depEvalResult = this.evaluate(origArptIcao, depWeatherData, causeCode);
						}
						
					} else {
						
						// 출발원인에 대한 기상정보
						depWeatherData = this.getDepWeatherData(origArptIcao, schdDtDepStr);
						
						// 도착원인에 대한 기상정보
						arrWeatherData = this.getArrWeatherData(destArptIcao, schdDtDepStr, schdDtArrStr);
								
						// 출발공항 기상정보가 없는 경우 무효
						if (depWeatherData.get("metarDataList") == null) {

							depEvalResult = -1;

						} else {
							// 출발원인에 대한 평가
							depEvalResult = this.evaluate(origArptIcao, depWeatherData, causeCode);
						}
						
						// 도착공항 기상정보가 없는 경우 무효
						if(arrWeatherData.get("metarDataList") == null) {
							
							arrEvalResult = -1;
							
						} else {
							// 도착원인에 대한 평가
							arrEvalResult = this.evaluate(destArptIcao, arrWeatherData, causeCode);							
						}						
					}
				}	
				
				// 출발공항 원인이라면
				if("DEP".equals(causeAirport)) {
                    
					arrEvalResult = 2;

					evalResult = depEvalResult;
                   
                // 도착공항 원인이라면
				} else if("ARR".equals(causeAirport)) {

					depEvalResult = 2;
					
					evalResult = arrEvalResult;
					
				// 원인공항이 특정되지 않는다면 양쪽 모두
				} else {
					
					// 출발공항이 무효라면
					if(depEvalResult == -1) {
						
						if(arrEvalResult == -1) {
                            evalResult = -1;
						} else if(arrEvalResult == 2) {
                            evalResult = -1;
						} else {
							evalResult = arrEvalResult;
						}
					
					// 출발공항이 평가 대상이 아니라면
					} else if(depEvalResult == 2) {
						
						if(arrEvalResult == -1) {
                            evalResult = -1;
						} else if(arrEvalResult == 2) {
                            evalResult = 2;
						} else {
							evalResult = arrEvalResult;
						}
						
					} else if(arrEvalResult == -1) {
						
						if(depEvalResult == -1) {
                            evalResult = -1;
						} else if(depEvalResult == 2) {
                            evalResult = -1;
						} else {
							evalResult = depEvalResult;
						}			
						
					} else if(arrEvalResult == 2) {
						
						if(depEvalResult == -1) {
                            evalResult = -1;
						} else if(depEvalResult == 2) {
                            evalResult = 2;
						} else {
							evalResult = depEvalResult;
						}					
					} else {

						if (depEvalResult == 1 && arrEvalResult == 1) {
							evalResult = 1;
						} else {
							evalResult = 0;
						}
					}
				}
							
				if(evalResult == 0) {
				
					System.out.println("-> 평가 결과 : 불발");
					
				} else if(evalResult == 1){
					
					System.out.println("-> 평가 결과 : 적중");
						
				} else if(evalResult == -1) {
					    
                    System.out.println("-> 평가 결과 : 무효");
                    
				} else if(evalResult == 2) {
					    
                    System.out.println("-> 평가 결과 : 평가대상 아님");
				}
				
				Map<String, String> insertMap = new HashMap<String, String>();
				
				insertMap.put("SEQ", fltPk);
				insertMap.put("AIRLINE", fltCarrKor);
				insertMap.put("DEP_PLAN_TIME", schdDtDepStr);
				insertMap.put("DEP_REAL_TIME", actlDtDepStr);
				insertMap.put("DEP_RAMP_TIME", estmDtDepStr);
				insertMap.put("ARR_PLAN_TIME", schdDtArrStr);
				insertMap.put("ARR_REAL_TIME", actlDtArrStr);
				insertMap.put("ARR_RAMP_TIME", actlDtArrStr);
				insertMap.put("DEP_AIRPORT", origArptKor);
				insertMap.put("DEP_STN_CD", origArptIcao);
				insertMap.put("ARR_AIRPORT", destArptKor);
				insertMap.put("ARR_STN_CD", destArptIcao);
				insertMap.put("FLIGHT_NUMBER", fltId);
				insertMap.put("STATUS", statCode);
				insertMap.put("CAUSE", cause);
				insertMap.put("CAUSE_CODE", causeCode + "");
				insertMap.put("CAUSE_AIRPORT", causeAirport);
				insertMap.put("LINE_KIND", "1");
				insertMap.put("DEP_EVAL_RESULT", depEvalResult + "");
				insertMap.put("ARR_EVAL_RESULT", arrEvalResult + "");
				insertMap.put("EVAL_RESULT", evalResult + "");
				
				Map<String, String> depWeatherDataString = this.createWeatherDataString(depWeatherData);
				Map<String, String> arrWeatherDataString = this.createWeatherDataString(arrWeatherData);
				
				if (depWeatherDataString != null) {
					insertMap.put("DEP_TAF_DATA", depWeatherDataString.get("tafDataString"));
					insertMap.put("DEP_METAR_DATA", depWeatherDataString.get("metarDataString"));
					insertMap.put("DEP_TAF_UID", depWeatherDataString.get("tafUid"));
					insertMap.put("DEP_METAR_UIDS", depWeatherDataString.get("metarUids"));
				}
				
				if (arrWeatherDataString != null) {
					insertMap.put("ARR_TAF_DATA", arrWeatherDataString.get("tafDataString"));
					insertMap.put("ARR_METAR_DATA", arrWeatherDataString.get("metarDataString"));
					insertMap.put("ARR_TAF_UID", arrWeatherDataString.get("tafUid"));
					insertMap.put("ARR_METAR_UIDS", arrWeatherDataString.get("metarUids"));
				}
				
				this.insertFoisLocalEvalInfo(insertMap);
			}
			
			dbManager.commit();

		} catch (Exception e) {

			e.printStackTrace();
			
			dbManager.rollback();
			
		} finally {
			
		}
		
		dbManager.safeClose();
	}
	
	public String getAirlineKorbyIATA(String iataCode) {
		
		if(iataCode == null) {
			return null;
		}
		
		switch(iataCode) {
		
		case "KE":
			return "대한항공";
		case "OZ":
			return "아시아나항공";
		case "7C":
			return "제주항공";
		case "LJ":
			return "진에어";
		case "BX":
			return "에어부산";
		case "ZE":
			return "이스타항공";
		case "RS":
			return "에어서울";
		case "TW":
			return "티웨이항공";
		case "KJ":
			return "에어인천";
		case "4V":
			return "플라이강원";
		case "YP":
			return "에어프레미아";
		case "RF":
			return "에어로케이항공";
		case "4H":
			return "하이에어";
		case "XE":
			return "코리아익스프레스에어";
		}
		
		return iataCode;
	}
	
	private String convertUTCtoKST(String utcTmStr) {
		
		try {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(sdf.parse(utcTmStr));
			cal.add(Calendar.HOUR_OF_DAY, 9);
			
			return sdf.format(cal.getTime());
			
		} catch (Exception e) {
			return null;
		}
	}
	
	private void insertFoisLocalEvalInfo(Map<String, String> insertMap) {
	
		
		String query = "";
		
		query += "INSERT INTO FILTERED_FOIS_LOCAL_EVAL_INFO (";
		query += "SEQ, AIRLINE, DEP_PLAN_TIME, DEP_REAL_TIME, DEP_RAMP_TIME, ARR_PLAN_TIME, ARR_REAL_TIME, ARR_RAMP_TIME, DEP_AIRPORT, DEP_STN_CD, ARR_AIRPORT, ARR_STN_CD, FLIGHT_NUMBER, STATUS, CAUSE, CAUSE_CODE, CAUSE_AIRPORT, LINE_KIND, DEP_EVAL_RESULT, ARR_EVAL_RESULT, EVAL_RESULT, DEP_TAF_DATA, DEP_METAR_DATA, ARR_TAF_DATA, ARR_METAR_DATA, DEP_TAF_UID, DEP_METAR_UIDS, ARR_TAF_UID, ARR_METAR_UIDS";
		query += ") VALUES (";
		
		query += "'" + insertMap.get("SEQ") + "',";
		query += "'" + insertMap.get("AIRLINE") + "',";
		query += "TO_DATE('" + insertMap.get("DEP_PLAN_TIME") + "','YYYYMMDDHH24MI'),";
		query += "TO_DATE('" + insertMap.get("DEP_REAL_TIME") + "','YYYYMMDDHH24MI'),";
		query += "TO_DATE('" + insertMap.get("DEP_RAMP_TIME") + "','YYYYMMDDHH24MI'),";
		query += "TO_DATE('" + insertMap.get("ARR_PLAN_TIME") + "','YYYYMMDDHH24MI'),";
		query += "TO_DATE('" + insertMap.get("ARR_REAL_TIME") + "','YYYYMMDDHH24MI'),";
		query += "TO_DATE('" + insertMap.get("ARR_RAMP_TIME") + "','YYYYMMDDHH24MI'),";
		query += "'" + insertMap.get("DEP_AIRPORT") + "',";
		query += "'" + insertMap.get("DEP_STN_CD") + "',";
		query += "'" + insertMap.get("ARR_AIRPORT") + "',";
		query += "'" + insertMap.get("ARR_STN_CD") + "',";
		query += "'" + insertMap.get("FLIGHT_NUMBER") + "',";
		query += "'" + insertMap.get("STATUS") + "',";
		query += "'" + insertMap.get("CAUSE") + "',";
		query += "'" + insertMap.get("CAUSE_CODE") + "',";
		query += "'" + insertMap.get("CAUSE_AIRPORT") + "',";
		query += "'" + insertMap.get("LINE_KIND") + "',";
		query += "'" + insertMap.get("DEP_EVAL_RESULT") + "',";
		query += "'" + insertMap.get("ARR_EVAL_RESULT") + "',";
		query += "'" + insertMap.get("EVAL_RESULT") + "',";
		query += "'" + insertMap.get("DEP_TAF_DATA") + "',";
		query += "'" + insertMap.get("DEP_METAR_DATA") + "',";
		query += "'" + insertMap.get("ARR_TAF_DATA") + "',";
		query += "'" + insertMap.get("ARR_METAR_DATA") + "',";
		query += "'" + insertMap.get("DEP_TAF_UID") + "',";
		query += "'" + insertMap.get("DEP_METAR_UIDS") + "',";
		query += "'" + insertMap.get("ARR_TAF_UID") + "',";
		query += "'" + insertMap.get("ARR_METAR_UIDS") + "'";
		
		query += ")";
		
		query = query.replaceAll("'null'", "null");
		
		this.dbManager.executeUpdate(query, false);		
	}
	
	private Map<String, String> createWeatherDataString(Map<String, Object> weatherData) {

		if (weatherData == null) {
			return null;
		}
		
		Map<String, String> weatherDataString = new HashMap<String, String>();
			
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
		
		if(tafData == null) {
			return null;
		}
		
		String tafDataString = "";
		String tafUid = "";
		
		tafUid += tafData.get("TAF_UID");
		tafDataString += tafData.get("TAF_TM") + ",";
		tafDataString += tafData.get("VIS") + ",";
		tafDataString += tafData.get("WSPD") + ",";
		tafDataString += tafData.get("WDIR") + ",";
		tafDataString += tafData.get("MAXWSPD") + ",";
		tafDataString += tafData.get("SIDEWIND") + ",";
		tafDataString += tafData.get("SKYCONDITION") + ",";
		tafDataString += tafData.get("CLOUD_AMOUNT_LAYER_1") + ",";
		tafDataString += tafData.get("CLOUD_HEIGHT_LAYER_1");
		
		weatherDataString.put("tafUid", tafUid);
		weatherDataString.put("tafDataString", tafDataString);
		
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if (metarDataList == null) {
			return weatherDataString;
		}
		
		String metarDataString = "";
		String metarUids = "";
		
		for(int i=0 ; i<metarDataList.size() ; i++) {
			
			Map<String, Object> metarData = metarDataList.get(i);
			
			metarUids += metarData.get("METAR_UID") + "|";
			metarDataString += metarData.get("MSG_TYPE") + ",";
			metarDataString += metarData.get("METAR_TM") + ",";
			metarDataString += metarData.get("VIS") + ",";
			metarDataString += metarData.get("WSPD") + ",";
			metarDataString += metarData.get("WDIR") + ",";
			metarDataString += metarData.get("MAXWSPD") + ",";
			metarDataString += metarData.get("SIDEWIND") + ",";
			metarDataString += metarData.get("SKYCONDITION") + ",";
			metarDataString += metarData.get("CLOUD_AMOUNT_LAYER_1") + ",";
			metarDataString += metarData.get("CLOUD_HEIGHT_LAYER_1");
			metarDataString += "|";
		}
		
		weatherDataString.put("metarDataString", metarDataString);
		
		weatherDataString.put("metarUids", metarUids);

		return weatherDataString;
	}
	
	// causeCode 에 따라 평가 진행, -1: 무효, 0: 불발, 1: 적중
	private int evaluate(String stnCd, Map<String, Object> weatherData, Integer causeCode) {
		
		/*
		 * 측풍: 1
		 * 눈: 2
		 * 뇌우: 3
		 * 시정(실링포함): 4
		 * 시정: 5		 
		 * 실링: 6
		 * 태풍: 7
		 */
		
		int evalResult = -1;
		
		switch(causeCode) {
		
			case 1:
				
				evalResult = this.evaluateSideWind(stnCd, weatherData);
				break;
				
			case 2:
				
				evalResult = this.evaluateSnow(stnCd, weatherData);
				break;
				
			case 3:

				evalResult = this.evaluateTs(stnCd, weatherData);
				break;
				
			case 4:
				
			{
				
				int evalResult1 = this.evaluateVis(stnCd, weatherData);
				int evalResult2 = this.evaluateCeiling(stnCd, weatherData);				
				
				// 결과중 하나라도 무효면 전체 무효
				if (evalResult1 > -1 && evalResult2 > -1) {
					
					if (evalResult1 == 1 && evalResult2 == 1) {
						evalResult = 1;
					} else {
						evalResult = 0;
					}					
				}
			}
				
				break;
				
			case 5:
			
				evalResult = this.evaluateVis(stnCd, weatherData);
				break;
				
			case 6:
				
				evalResult = this.evaluateCeiling(stnCd, weatherData);				
				break;
				
			case 7:
			
			{				
				
				int evalResult1 = this.evaluateSideWind(stnCd, weatherData);
				int evalResult2 = this.evaluateVis(stnCd, weatherData);
				int evalResult3 = this.evaluateCeiling(stnCd, weatherData);
				
				// 결과중 하나라도 무효면 전체 무효
				if (evalResult1 > -1 && evalResult2 > -1 && evalResult3 > -1) {
					
					if (evalResult1 == 1 && evalResult2 == 1 && evalResult3 == 1) {
						evalResult = 1;
					} else {
						evalResult = 0;
					}					
				}
			}	
				break;	
		}
	
		return evalResult;
	}
	
	private int evaluateVis(String stnCd, Map<String, Object> weatherData) {
		
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
			
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if(tafData == null || metarDataList == null) {
			return -1;
		}
		
		Integer tafVis = Integer.valueOf(tafData.get("VIS").toString());
		
		int evalResult = 0;
		
		for (Map<String, Object> metarData : metarDataList) {

			Integer metarVis = Integer.valueOf(metarData.get("VIS").toString());

			switch(stnCd.trim().toUpperCase()) {
			
			case "RKSI":
			case "RKSS":
				
				if ((metarVis <= 200) && (tafVis <= 200)) {					
					return 1;
				}
			
				if ((metarVis > 200 && metarVis <= 300) && (tafVis > 200 && tafVis <= 300))  {
					return 1;
				}
			
			
				if ((metarVis > 300 && metarVis <= 500) && (tafVis > 300 && tafVis <= 500)) {
					return 1;
				}
			
				if ((metarVis > 500) && (tafVis > 500)) {					
					return 1;
				}
				
				break;
				
			case "RKPC":
				
				if ((metarVis <= 300) && (tafVis <= 300)) {					
					return 1;
				}
			
				if ((metarVis > 300 && metarVis <= 500) && (tafVis > 300 && tafVis <= 500))  {
					return 1;
				}
			
			
				if ((metarVis > 500 && metarVis <= 700) && (tafVis > 500 && tafVis <= 700)) {
					return 1;
				}
			
				if ((metarVis > 700) && (tafVis > 700)) {					
					return 1;
				}
				
				break;	
				
			case "RKPU":
				
				if ((metarVis <= 500) && (tafVis <= 500)) {					
					return 1;
				}
			
				if ((metarVis > 500 && metarVis <= 700) && (tafVis > 500 && tafVis <= 700))  {
					return 1;
				}
			
			
				if ((metarVis > 700 && metarVis <= 1500) && (tafVis > 700 && tafVis <= 1500)) {
					return 1;
				}
			
				if ((metarVis > 1500) && (tafVis > 1500)) {					
					return 1;
				}
				
				break;		
				
			case "RKJB":
			case "RKJY":
			case "RKNY":
				
				if ((metarVis <= 500) && (tafVis <= 500)) {					
					return 1;
				}
			
				if ((metarVis > 500 && metarVis <= 700) && (tafVis > 500 && tafVis <= 700))  {
					return 1;
				}
			
				if ((metarVis > 700) && (tafVis > 700)) {					
					return 1;
				}
				
				break;	
				
			default:
				return -1;
			}
		}
		
		return evalResult;
	}
	
	private int evaluateTs(String stnCd, Map<String, Object> weatherData) {
		
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
		
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if(tafData == null || metarDataList == null) {
			return -1;
		}
		
		int evalResult = -1;
		
		String tafSkyCondition = (String) tafData.get("SKYCONDITION");
		
		if (tafSkyCondition == null) {
			tafSkyCondition = "";
		}
		
		for (int i = 0; i < metarDataList.size(); i++) {

			String metarSkyCondition = (String) metarDataList.get(i).get("SKYCONDITION");

			if (metarSkyCondition == null) {
				metarSkyCondition = "";
			}
			
			// Metar 와 Taf 모두 현상이 없다면 평가 X
			if (!metarSkyCondition.contains("TS") && !tafSkyCondition.contains("TS")) {
				continue;
				
			// Metar 와 Taf 둘중의 하나라도 현상이 있다면 평가 진행
			} else if (metarSkyCondition.contains("TS") || tafSkyCondition.contains("TS")) {
			
				if (metarSkyCondition.contains("TS") && tafSkyCondition.contains("TS")) {
					return 1;
				} else {
					evalResult = 0;
				}						
			}
		}		
		
		return evalResult;
	}
	
	private int evaluateSnow(String stnCd, Map<String, Object> weatherData) {
				
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
		
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if(tafData == null || metarDataList == null) {
			return -1;
		}
		
		int evalResult = -1;
		
		String tafSkyCondition = (String) tafData.get("SKYCONDITION");
		
		if (tafSkyCondition == null) {
			tafSkyCondition = "";
		}
		
		for (int i = 0; i < metarDataList.size(); i++) {

			String metarSkyCondition = (String) metarDataList.get(i).get("SKYCONDITION");

			if (metarSkyCondition == null) {
				metarSkyCondition = "";
			}
			
			// Metar 와 Taf 모두 현상이 없다면 평가 X
			if (!metarSkyCondition.contains("SN") && !tafSkyCondition.contains("SN")) {
				continue;
				
			// Metar 와 Taf 둘중의 하나라도 현상이 있다면 평가 진행
			} else if (metarSkyCondition.contains("SN") || tafSkyCondition.contains("SN")) {
			
				if (metarSkyCondition.contains("SN") && tafSkyCondition.contains("SN")) {
					return 1;
				} else {
					evalResult = 0;
				}						
			}
		}		
		
		return evalResult;
	}
	
	private int evaluateSideWind(String stnCd, Map<String, Object> weatherData) {
				
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
		
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if(tafData == null || metarDataList == null) {
			return -1;
		}
		
		int evalResult = -1;
		
		Integer tafWspd = Integer.valueOf(tafData.get("WSPD").toString());
		Integer tafMaxWspd = tafData.get("MAXWSPD") == null ? null : Integer.valueOf(tafData.get("MAXWSPD").toString());
		Integer tafWdir = Integer.valueOf(tafData.get("WDIR").toString());
		
		double tafSideWind = this.getSideWinds(stnCd, tafWspd, tafMaxWspd, tafWdir);
		
		tafData.put("SIDEWIND", tafSideWind);

		for(int i=0 ; i<metarDataList.size() ; i++) {
			
			Map<String, Object> metarData = metarDataList.get(i);
			
			Integer metarWspd = Integer.valueOf(metarData.get("WSPD").toString());
			Integer metarMaxWspd = metarData.get("MAXWSPD") == null ? null : Integer.valueOf(metarData.get("MAXWSPD").toString());
			Integer metarWdir = Integer.valueOf(metarData.get("WDIR").toString());
			
			double metarSideWind = this.getSideWinds(stnCd, metarWspd, metarMaxWspd, metarWdir);
			
			metarDataList.get(i).put("SIDEWIND", metarSideWind);
			
			// Metar 와 Taf 측풍이 30KT 미만이라면 무효
			if(metarSideWind < 30 && tafSideWind < 30) {
				continue;
			} else if (metarSideWind >= 30 || tafSideWind >= 30) {
              
				if (metarSideWind >= 30 && tafSideWind >= 30) {
					return 1;
				} else {
					evalResult = 0;
				}
			}
		}		
		
		return evalResult;
	}
	
	private int evaluateCeiling(String stnCd, Map<String, Object> weatherData) {
		
		// 평가 미대상 공항은 무효 리턴
		if (!"RKPC".equals(stnCd) && !"RKPU".equals(stnCd)) {
			return -1;
		}
		
		Map<String, Object> tafData = (Map<String, Object>)weatherData.get("tafData");
		
		List<Map<String, Object>> metarDataList = (List<Map<String, Object>>)weatherData.get("metarDataList");
		
		if(tafData == null || metarDataList == null) {
			return -1;
		}
		
		int evalResult = -1;
		
		String tafCloudAmountLayer1 = (String) tafData.get("CLOUD_AMOUNT_LAYER_1");
		String tafCloudHeightLayer1 = (String) tafData.get("CLOUD_HEIGHT_LAYER_1");
		
		String tafCeiling = this.getCeiling(tafCloudAmountLayer1, tafCloudHeightLayer1);
		
		String tafCeilingAmount = null;
		String tafCeilingHeight = null;
		
		if (tafCeiling != null) {
			
			tafCeilingAmount = tafCeiling.split(" ")[0];
			tafCeilingHeight = tafCeiling.split(" ")[1];			
		}
		
		for (Map<String, Object> metarData : metarDataList) {
			
			String metarCloudAmountLayer1 = (String) metarData.get("CLOUD_AMOUNT_LAYER_1");
			String metarCloudHeightLayer1 = (String) metarData.get("CLOUD_HEIGHT_LAYER_1");
			
			String metarCeiling = this.getCeiling(metarCloudAmountLayer1, metarCloudHeightLayer1);
			
			String metarCeilingAmount = null;
			String metarCeilingHeight = null;
			
			// Metar 와 Taf 모두 실링이 없으면 무효
			if (metarCeiling == null && tafCeiling == null) {
				continue;
			// Metar 와 Taf 둘중의 하나 이상 실링이 있으면 평가
			} else if (metarCeiling != null || tafCeiling != null) {
				
				// Metar 만 실링이 있는 경우
				if (metarCeiling != null && tafCeiling == null) {

					metarCeilingAmount = metarCeiling.split(" ")[0];
					metarCeilingHeight = metarCeiling.split(" ")[1];
										
					if("RKPC".equals(stnCd)) {
						
						// 실링이 200 초과면 무효
						if(Double.valueOf(metarCeilingHeight) > 200) {
							continue;
						} else {
							evalResult = 0;
						}
						
					} else if ("RKPU".equals(stnCd)) {

						// 실링이 800 초과면 무효
						if(Double.valueOf(metarCeilingHeight) > 800) {
							continue;
						} else {
							evalResult = 0;
						}						
					}
					
				} else if (metarCeiling == null && tafCeiling != null) {

					if("RKPC".equals(stnCd)) {
						
						// 실링이 200 초과면 무효
						if(Double.valueOf(tafCeilingHeight) > 200) {
							continue;
						} else {
							evalResult = 0;
						}
						
					} else if ("RKPU".equals(stnCd)) {

						// 실링이 800 초과면 무효
						if(Double.valueOf(tafCeilingHeight) > 800) {
							continue;
						} else {
							evalResult = 0;
						}						
					}
					
				} else if(metarCeiling != null && tafCeiling != null) {
					
					// Metar 와 Taf 둘 모두 실링이 있는 경우 평가
					
					metarCeilingAmount = metarCeiling.split(" ")[0];
					metarCeilingHeight = metarCeiling.split(" ")[1];
					
					if("RKPC".equals(stnCd)) {
						
						// 모두 실링이 200 초과면 무효
						if(Double.valueOf(tafCeilingHeight) > 200 && Double.valueOf(metarCeilingHeight) > 200) {
							continue;
						} else {
							
							// 모두 실링이 200이하면 적중
							if (Double.valueOf(tafCeilingHeight) <= 200 && Double.valueOf(metarCeilingHeight) <= 200) {
								return 1;
							} else {
								evalResult = 0;
							}
						}
						
					} else if ("RKPU".equals(stnCd)) {


						// 모두 실링이 800 초과면 무효
						if (Double.valueOf(tafCeilingHeight) > 800 && Double.valueOf(metarCeilingHeight) > 800) {
							continue;
						} else {

							// 모두 실링이 800이하면 적중
							if (Double.valueOf(tafCeilingHeight) <= 800 && Double.valueOf(metarCeilingHeight) <= 800) {
								return 1;
							} else {
								evalResult = 0;
							}
						}						
					}					
				} 
			}
		}
		
		return evalResult;
	}
	
	private double getSideWinds(String stnCd, Integer wspd, Integer maxWspd, Integer wdir) {
		
		String[][] airportRwyInfos = this.getAirportRwyInfos(stnCd);
		
		String[] airportRwyInfo = airportRwyInfos[0];
		
		double rwyDir = Double.valueOf(airportRwyInfo[1]);
		
		double sideWdDegree = Math.abs(rwyDir - Double.valueOf(wdir)) % 360;
		
		double sideWind = Math.abs((Double.valueOf(wspd) * Math.sin(Math.toRadians(sideWdDegree))));
		
		if(maxWspd != null) {
			sideWind = Math.abs((Double.valueOf(maxWspd) * Math.sin(Math.toRadians(sideWdDegree))));
		}
		
		return sideWind;
	}
	
	private String getCeiling(String cloudAmountLayer1, String cloudHeightLayer1) {
		
		if (cloudAmountLayer1 == null || cloudHeightLayer1 == null) {
			return null;
		}
		
		String lowestCloundAmount = null;
		String lowestCloundHeight = null;
		
		String[] cloudAmounts = cloudAmountLayer1.split(" ");
		String[] cloudHeights = cloudHeightLayer1.split(" ");
		
		for(int i=0 ; i<cloudAmounts.length ; i++) {
			
			String cloudAmount = cloudAmounts[i];
			String cloudHeight = cloudHeights[i];
			
			if (cloudAmount.equals("BKN") || cloudAmount.equals("OVC")) {

				if (lowestCloundAmount == null) {
					lowestCloundAmount = cloudAmount;
					lowestCloundHeight = cloudHeight;
				}
			}
		}
		
		if (lowestCloundAmount != null) {
			return lowestCloundAmount + " " + lowestCloundHeight;
		} else {
			return null;
		}
	}
	
	// 출발공항 원인일 경우 기상 데이터
	private Map<String, Object> getDepWeatherData(String depStnCd, String depPlanTimeStr) throws Exception {

		Map<String, Object> depWeatherData = new HashMap<String, Object>();
		
		Map<String, Object> tafData = this.getTafData(depStnCd, depPlanTimeStr, depPlanTimeStr);
		
		List<Map<String, Object>> metarDataList = this.getMetarDataList(depStnCd, depPlanTimeStr);
		
		if (metarDataList.size() == 0) {
			metarDataList = null;
		}
		
		depWeatherData.put("tafData", tafData);
		depWeatherData.put("metarDataList", metarDataList);
		
		return depWeatherData;
	}
	
	// 도착공항 원인일 경우 기상 데이터
	private Map<String, Object> getArrWeatherData(String arrStnCd, String depPlanTimeStr, String arrPlanTimeStr) throws Exception {
		
		Map<String, Object> arrWeatherData = new HashMap<String, Object>();

		Map<String, Object> tafData = this.getTafData(arrStnCd, depPlanTimeStr, arrPlanTimeStr);
		
		List<Map<String, Object>> metarDataList = this.getMetarDataList(arrStnCd, arrPlanTimeStr);
		
		if (metarDataList.size() == 0) {
			metarDataList = null;
		}
		
		arrWeatherData.put("tafData", tafData);
		arrWeatherData.put("metarDataList", metarDataList);
		
		return arrWeatherData;
	}
	
	
	// 기본 데이터 추출
	private List<Map<String, Object>> getFoisLocalInfoDataList(String startTimeStr, String endTimeStr) throws Exception {
	
		String query = ""+
				
			" SELECT                                                     			      			"+
			"   A.FLT_PK,                                                   			  			"+
			"   A.FLT_CARR,                                               			      			"+
			"   TO_CHAR(A.SCHD_DT_DEP, 'YYYYMMDDHH24MI') AS SCHD_DT_DEP,     			  			"+
			"   TO_CHAR(A.ACTL_DT_DEP, 'YYYYMMDDHH24MI') AS ACTL_DT_DEP,     			  			"+
			"   TO_CHAR(A.ESTM_DT_DEP, 'YYYYMMDDHH24MI') AS ESTM_DT_DEP,                  			"+
			"   TO_CHAR(A.SCHD_DT_ARR, 'YYYYMMDDHH24MI') AS SCHD_DT_ARR,     			  			"+
			"   TO_CHAR(A.ACTL_DT_ARR, 'YYYYMMDDHH24MI') AS ACTL_DT_ARR,     			  			"+
			"   TO_CHAR(A.ESTM_DT_ARR, 'YYYYMMDDHH24MI') AS ESTM_DT_ARR, 			      			"+
			"   A.ORIG_ARPT,                                                              			"+
			"   A.ORIG_ARPT_ICAO, 		                                                  			"+
			"   A.ORIG_ARPT_KOR, 		                                                  			"+
			"   A.DEST_ARPT,                                           			          			"+
			"   A.DEST_ARPT_ICAO,         		                                          			"+
			"   A.DEST_ARPT_KOR,         		                                          			"+
			"   A.FLT_ID,                                         			              			"+
			"   A.DEP_STAT,                                                               			"+
			"   A.DEP_RSON_CODE,                                                          			"+
			"   A.ARR_STAT,                                                               			"+
			"   A.ARR_RSON_CODE                                          			      			"+
			" FROM AAMI.FILTERED_FOIS_LOCAL A                                             			"+
			" WHERE 1=1                                                                   			"+
			" AND (A.DEP_STAT = 'CNL' OR A.DEP_STAT = 'DLA' OR A.DEP_STAT = 'DIV' OR      			"+
			"      A.ARR_STAT = 'CNL' OR A.ARR_STAT = 'DLA' OR A.ARR_STAT = 'DIV')        			"+             				
			" AND A.SCHD_DT_DEP >= TO_DATE('"+startTimeStr+"', 'YYYYMMDDHH24MI')              		"+
			" AND A.SCHD_DT_DEP <= TO_DATE('"+endTimeStr+"', 'YYYYMMDDHH24MI') 	          			"+
			" AND (A.ORIG_ARPT_ICAO IN ('RKSI', 'RKSS', 'RKPC', 'RKPU', 'RKJB', 'RKJY', 'RKNY')     "+
			"   OR A.DEST_ARPT_ICAO IN ('RKSI', 'RKSS', 'RKPC', 'RKPU', 'RKJB', 'RKJY', 'RKNY'))    "+
			" AND (A.DEP_RSON_CODE IN ('WV','WW','WH','WN','WC','WD','WU')                			"+
			"   OR A.ARR_RSON_CODE IN ('WV','WW','WH','WN','WC','WD','WU'))               			"+
			" ORDER BY A.SCHD_DT_DEP ASC                                                  			";				
		
		ResultSet resultSet = dbManager.executeQuery(query);
		
		List<Map<String, Object>> foisLocalInfoDataList = new ArrayList<Map<String, Object>>();
		
		while(resultSet.next()) {
			Map<String, Object> foisLocalInfoData = DaemonUtils.getResultSetData(resultSet);	
			foisLocalInfoDataList.add(foisLocalInfoData);
		}
			
		return foisLocalInfoDataList;
	}
	
	public Map<String, Object> getTafData(String stnCd, String tmStr, String tafTmStr) throws Exception {
		
		// tmstr 은 KST, taf 는 UTC
		
		String query = ""+
				" SELECT                                                          		"+
				" 	A.TAF_UID,	                                                        "+
				" 	TO_CHAR(B.TAF_TM, 'YYYYMMDDHH24MI') AS TAF_TM,               		"+
				" 	B.VIS,																"+
				" 	B.WSPD,																"+
				" 	B.WDIR,																"+
				" 	B.MAXWSPD,															"+
				" 	B.SKYCONDITION,														"+
				" 	B.CLOUD_AMOUNT_LAYER_1,												"+
				" 	B.CLOUD_HEIGHT_LAYER_1												"+
				" FROM AAMI.AMIS_TAF A                                               	"+
				" INNER JOIN AAMI.AMIS_TAF_DECODE B                                  	"+
				" ON A.TAF_UID = B.TAF_UID                                           	"+
				" WHERE STN_CD = '"+stnCd+"'                                         	"+
				" AND B.STATE = 'FCST'                                              	"+
				" AND A.TM <= TO_DATE('"+tmStr+"','YYYYMMDDHH24MI')-4/24-9/24      	 	"+
				" AND B.TAF_TM <= TO_DATE('"+tafTmStr+"','YYYYMMDDHH24MI')-9/24       	"+
				" AND B.TAF_TM >= TO_DATE('"+tafTmStr+"','YYYYMMDDHH24MI')-1/24-9/24 	"+
				" ORDER BY A.TM DESC, B.TAF_TM DESC                                  	";		
					
		ResultSet resultSet = dbManager.executeQuery(query);
		
		List<Map<String, Object>> tafDataList = new ArrayList<Map<String, Object>>();
		
		while(resultSet.next()) {
			Map<String, Object> tafData = DaemonUtils.getResultSetData(resultSet);	
			tafDataList.add(tafData);
		}
		
		if (tafDataList.size() > 0) {
			return tafDataList.get(0);
		}
		
		return null;
	}
	
	public List<Map<String, Object>> getMetarDataList(String stnCd, String tmStr) throws Exception {
		
		// tmstr 은 KST, metar 는 UTC
		
		String query = ""+
				" SELECT                                                                	    "+
				" 	A.METAR_UID,                                                           	    "+
				" 	A.MSG_TYPE,                                                           	    "+
				" 	TO_CHAR(B.METAR_TM, 'YYYYMMDDHH24MI') AS METAR_TM,               			"+
				" 	B.VIS,																		"+
				" 	B.WSPD,																		"+
				" 	B.WDIR,																		"+
				" 	B.MAXWSPD,																	"+
				" 	B.SKYCONDITION,																"+
				" 	B.CLOUD_AMOUNT_LAYER_1,														"+
				" 	B.CLOUD_HEIGHT_LAYER_1														"+
				" FROM AAMI.AMIS_METAR A                                                        "+
				" INNER JOIN AAMI.AMIS_METAR_DECODE B                                           "+
				" ON A.METAR_UID = B.METAR_UID                                                  "+
				" WHERE STN_CD = '"+stnCd+"'                                                    "+
				" AND B.METAR_TM <= TO_DATE('"+tmStr+"','YYYYMMDDHH24MI') + 1/24/2 - 9/24       "+
				" AND B.METAR_TM >= TO_DATE('"+tmStr+"','YYYYMMDDHH24MI') - 1/24/2 - 9/24       "+
				" ORDER BY A.TM DESC, B.METAR_TM DESC                                           ";
	
		ResultSet resultSet = dbManager.executeQuery(query);
		
		List<Map<String, Object>> metarDataList = new ArrayList<Map<String, Object>>();
		
		while(resultSet.next()) {
			Map<String, Object> metarData = DaemonUtils.getResultSetData(resultSet);	
			metarDataList.add(metarData);
		}
		
		return metarDataList;
	}
	
	private String convertRsonCodeKor(String rsonCode) {
		
		String cause = "해당없음";
		
		if(rsonCode == null) {
			return cause;
		}
    	
    	switch(rsonCode) {
    	
	    	case "WG": cause = "출발공항 지상조업(악기상)"; break;
	    	case "WV": cause = "출발공항 시정 악화(운고 포함)"; break;
	    	case "WW": cause = "출발공항 강한 바람"; break;
	    	case "WH": cause = "출발공항 태풍"; break;
	    	case "WN": cause = "목적공항 눈"; break;
	    	case "WC": cause = "목적공항 시정악화"; break;
	    	case "WD": cause = "목적공항 강한바람"; break;
	    	case "WU": cause = "목적공항 태풍"; break;
    	}
    	
    	return cause;
	}
	
	private String convertStatCode(String stat) {
		
		if(stat == null) {
			return "0";
		}
		
		switch(stat) {
		
		case "DLA": return "1";
		case "CNL": return "2";
		case "DIV": return "3";
		}
		
		return "0";
	}

    private Integer determineCauseCodes(String cause) {
	 	
		/*
		 * 측풍: 1
		 * 눈: 2
		 * 뇌우: 3
		 * 시정(실링포함): 4
		 * 시정: 5		 
		 * 실링: 6
		 * 태풍: 7
		 */
		
		String[][] containWordData = new String[][]{
			{"바람"},
            {"눈"},
            {"뇌전","악기상"},
            {"운고 포함"},
            {"시정"},            
            {"운고"},
            {"태풍", "태퓽"}
		};
		
		for (int i = 0; i < containWordData.length; i++) {

			for (int j = 0; j < containWordData[i].length; j++) {

				if (cause.contains(containWordData[i][j])) {
					return i + 1;
				}
			}
		}
		
		return 0;
	}
    
    private String[][] getAirportRwyInfos(String stnCd) {
  
    	String[][] airportRwyInfo = null;
    	
    	switch(stnCd.trim().toUpperCase()) {
    	
    	case "RKSI":
    		
    		airportRwyInfo = new String[][] {    			
    			{"15L-33R", "144.66", "324.67"},
    			{"15R-33L", "144.66", "324.67"},
    			{"16-34", "144.66", "324.67"}    			
    		};
    		
    		break;
    		
    	case "RKSS":
    		
    		airportRwyInfo = new String[][] {    			
    			{"14R-32L", "135", "315.02"},
    			{"14L-32R", "135.01", "315.03"} 			
    		};
    		
    		break; 
    		
    	case "RKPC":
    		
    		airportRwyInfo = new String[][] {
    			{"07-25", "58.46", "238.47"},
    			{"13-31", "125.62", "305.63"}    			
    		};
    		
    		break;
    		
    	case "RKJB":
    		
    		airportRwyInfo = new String[][] {    			
    			{"01-19", "359.67", "179.67"}    			
    		};
    		
    		break;	
    		
    	case "RKPU":
    		
    		airportRwyInfo = new String[][] {    			
    			{"18-36", "176.1", "356.1"}    			
    		};
    		
    		break;
    		
    	case "RKNY":
    		
    		airportRwyInfo = new String[][] {    			
    			{"15-33", "141.16", "321.16"}    			
    		};
    		
    		break;	
    		
    	case "RKJY":
    		
    		airportRwyInfo = new String[][] {    			
    			{"17-35", "158.03", "338.03"}    			
    		};
    		
    		break;	
    		
    	case "RKPK":
    		
    		airportRwyInfo = new String[][] {    			
    			{"18L-36R", "173.95", "353.95"},
    			{"18R-36L", "144.66", "353.95"} 			
    		};
    		
    		break;	
    	
    	case "RKTU":
    	
    		airportRwyInfo = new String[][] {    			
    			{"06L-24R", "52.42", "232.43"},
    			{"06R-24L", "52.43", "232.43"}    			
    		};
    		
    		break;
    		
    	case "RKTN":
    		
    		airportRwyInfo = new String[][] {    			
    			{"13L-31R", "124.23", "304.23"},
    			{"13R-31L", "124.23", "304.23"}    			
    		};
    		
    		break;	
    		
    	case "RKJJ":
    		
    		airportRwyInfo = new String[][] {    			
    			{"04R-22L", "29.93", "209.93"},
    			{"04L-22R", "29.93", "209.93"}    			
    		};
    		
    		break;		
    		
    	case "RKTH":
    	
    		airportRwyInfo = new String[][] {    			
    			{"10-28", "89.23", "269.25"}   			
    		};
    		
    		break;	
    		
    	case "RKPS":
    		
    		airportRwyInfo = new String[][] {    			
    			{"06R-24L", "55.59", "235.6"},
    			{"06L-24R", "55.58", "235.59"}    			
    		};
    		
    		break;	
    		
    	default:
    		
    		airportRwyInfo = new String[][] {    			
    			{"0-0", "0", "0"}    			
    		};
    		
    		break;	
    	}
    	
    	return airportRwyInfo;
    }
    


	public static void main(String[] args) {
		
		EvaluateFoisLocalInfo evaluateFoisLocalInfo = new EvaluateFoisLocalInfo();

		evaluateFoisLocalInfo.process();
	}
}
