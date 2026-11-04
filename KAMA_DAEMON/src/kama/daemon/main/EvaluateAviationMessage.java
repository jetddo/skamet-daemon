package kama.daemon.main;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kr.go.aviation.message.AviationWeatherMessageValidator;
import kr.go.aviation.message.MissingItem;

/**
 * 항공기상 전문 평가 프로그램
 *
 * @author jetddo
 * Created on 2017-01-03
 */
public class EvaluateAviationMessage {

    private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final String getAmisAirmetQuery =
            "SELECT                                      " +
            "    TO_CHAR(TM, 'YYYYMMDDHH24MI') AS TM,    " +
            "    STN_CD,                                 " +
            "    MSG_NO,                                 " +
            "    MSG_STS,                                " +
            "    MSG_TYPE,                               " +
            "    MTPH,                                   " +
            "    MSG_TEXT,                               " +
            "    MSG_SRC,                                " +
            "    ST_TM,                                  " +
            "    ED_TM,                                  " +
            "    LOC                                     " +
            "FROM AMISUSER.AIRMET                        " +
            "WHERE MSG_ID = 'AIRMET'                     " +
            "AND TM >= TO_DATE(?, 'YYYYMMDDHH24MI')      " +
            "AND TM <= TO_DATE(?, 'YYYYMMDDHH24MI')      " +
            "ORDER BY TM ASC                             ";

    private final String getAmisSigmetQuery =
            "SELECT                                      " +
            "    TO_CHAR(TM, 'YYYYMMDDHH24MI') AS TM,    " +
            "    STN_CD,                                 " +
            "    MSG_NO,                                 " +
            "    MSG_STS,                                " +
            "    MSG_TYPE,                               " +
            "    MTPH,                                   " +
            "    MSG_TEXT,                               " +
            "    MSG_SRC,                                " +
            "    ST_TM,                                  " +
            "    ED_TM,                                  " +
            "    LOC                                     " +
            "FROM AMISUSER.SIGMET                        " +
            "WHERE MSG_ID = 'SIGMET'                     " +
            "AND TM >= TO_DATE(?, 'YYYYMMDDHH24MI')      " +
            "AND TM <= TO_DATE(?, 'YYYYMMDDHH24MI')      " +
            "ORDER BY TM ASC                             ";

    private final String insertEvaluationResultQuery =
            "MERGE INTO EVAL_AVI_MSG_REGULAR_RESULT A " +
            "USING ( " +
            "    SELECT TO_DATE(?, 'YYYYMMDDHH24MI') AS TM, " +
            "           ? AS AVIATION_MSG_TYPE, " +
            "           ? AS STN_CD, " +
            "           ? AS MSG_NO, " +
            "           ? AS MSG_STS, " +
            "           ? AS MTPH, " +
            "           ? AS ST_TM, " +
            "           ? AS ED_TM, " +
            "           ? AS MSG_SRC, " +
            "           ? AS SCORE, " +
            "           ? AS EVAL_MESSAGE " +
            "    FROM DUAL " +
            ") B " +
            "ON ( " +
            "    A.TM = B.TM " +
            "    AND A.AVIATION_MSG_TYPE = B.AVIATION_MSG_TYPE " +
            "    AND A.STN_CD = B.STN_CD " +
            "    AND A.MSG_NO = B.MSG_NO " +
            ") " +
            "WHEN MATCHED THEN UPDATE SET " +
            "    A.MSG_STS = B.MSG_STS, " +
            "    A.MTPH = B.MTPH, " +
            "    A.ST_TM = B.ST_TM, " +
            "    A.ED_TM = B.ED_TM, " +
            "    A.MSG_SRC = B.MSG_SRC, " +
            "    A.SCORE = B.SCORE, " +
            "    A.EVAL_MESSAGE = B.EVAL_MESSAGE " +
            "WHEN NOT MATCHED THEN INSERT ( " +
            "    TM, AVIATION_MSG_TYPE, STN_CD, MSG_NO, " +
            "    MSG_STS, MTPH, ST_TM, ED_TM, MSG_SRC, SCORE, EVAL_MESSAGE " +
            ") VALUES ( " +
            "    B.TM, B.AVIATION_MSG_TYPE, B.STN_CD, B.MSG_NO, " +
            "    B.MSG_STS, B.MTPH, B.ST_TM, B.ED_TM, B.MSG_SRC, B.SCORE, B.EVAL_MESSAGE " +
            ")";

    private Configuration config;

    private DatabaseManager dbManager;

    private AmisDataBaseManager amisDbmanager;

    /**
     * 초기화
     */
    public boolean initialize() {
        Configurations configs = new Configurations();

        try {
            this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));

            this.dbManager = DatabaseManager.getInstance();
            this.dbManager.setConfig(new DaemonSettings(this.config));
            this.dbManager.setAutoCommit(false);

            this.amisDbmanager = new AmisDataBaseManager(this.config);

            return true;

        } catch (Exception e) {
            System.out.println(
                "Error : EvaluateAviationMessage.initialize -> " + e
            );
            e.printStackTrace();

            this.destroy();

            return false;
        }
    }

    /**
     * 자원 해제
     */
    public void destroy() {
        if (this.dbManager != null) {
            this.dbManager.safeClose();
        }

        if (this.amisDbmanager != null) {
            this.amisDbmanager.safeClose();
        }
    }

    /**
     * 평가 실행
     */
    private void process(Date startDate, Date endDate) {
    	
        System.out.println(this.logDateFormat.format(new Date()) + " -> ::::: Start EvaluateAviationMessage :::::");
        
        try {
        	
            this.evaluateAirmet(startDate, endDate);        	
        	
		} catch (Exception e) {
			System.out.println("Error : EvaluateAviationMessage.process -> " + e);
			e.printStackTrace();
		}

        try {
        	
        	this.evaluateSigmet(startDate, endDate);
        	
		} catch (Exception e) {
			System.out.println("Error : EvaluateAviationMessage.process -> " + e);
			e.printStackTrace();
		}

        
        System.out.println(this.logDateFormat.format(new Date()) + " -> ::::: End EvaluateAviationMessage :::::");
    }

	/**
	 * AIRMET 평가
	 */
	private void evaluateAirmet(Date startDate, Date endDate) {
	
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
	    String stDtStr = sdf.format(startDate);
	    String edDtStr = sdf.format(endDate);
	
	    long startMillis = System.currentTimeMillis();
	
	    int successCount = 0;
	    int failureCount = 0;
	
	    try {
	
	        List<Map<String, Object>> airmetDataList = this.getAmisAirmetDataList(stDtStr, edDtStr);
	
	        List<Map<String, Object>> evaluationResultList = new ArrayList<Map<String, Object>>();
	
	        for (Map<String, Object> data : airmetDataList) {
	
	            String msgSrc = (String) data.get("msgSrc");
	            List<MissingItem> missingItemList = AviationWeatherMessageValidator.validateAirmet(msgSrc);
	
	            String score = missingItemList.isEmpty() ? "100" : "0";
	            String evalMessage = this.getEvaluationMessage(missingItemList);
	
	            data.put("aviationMsgType", "AIRMET");
	            data.put("score", score);
	            data.put("evalMessage", evalMessage);
	
	            evaluationResultList.add(data);
	
	            if (missingItemList.isEmpty()) {
	                successCount++;
	            } else {
	                failureCount++;
	            }
	        }
	
	        // 평가 결과 저장
	        this.insertEvaluationResultList(evaluationResultList);
	
	        // 트랜잭션 확정
	        this.dbManager.commit();
	
	        long elapsedMillis = System.currentTimeMillis() - startMillis;
	
	        this.printEvaluationResultLog(
	                "AIRMET", startDate, endDate,
	                airmetDataList.size(), successCount, failureCount, elapsedMillis
	        );
	
	    } catch (Exception e) {
	
	        e.printStackTrace();
	
	        this.dbManager.rollback();
	
	        throw new RuntimeException("EvaluateAviationMessage.evaluateAirmet failed.", e);
	    }
	}

	/**
	 * SIGMET 평가
	 */
	private void evaluateSigmet(Date startDate, Date endDate) {
	
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
	    String stDtStr = sdf.format(startDate);
	    String edDtStr = sdf.format(endDate);
	
	    long startMillis = System.currentTimeMillis();
	
	    int successCount = 0;
	    int failureCount = 0;
	
	    try {
	
	        List<Map<String, Object>> sigmetDataList = this.getAmisSigmetDataList(stDtStr, edDtStr);
	
	        List<Map<String, Object>> evaluationResultList = new ArrayList<Map<String, Object>>();
	
	        for (Map<String, Object> data : sigmetDataList) {
	
	            String msgSrc = (String) data.get("msgSrc");
	            List<MissingItem> missingItemList = AviationWeatherMessageValidator.validateSigmet(msgSrc);
	
	            String score = missingItemList.isEmpty() ? "100" : "0";
	            String evalMessage = this.getEvaluationMessage(missingItemList);
	
	            data.put("aviationMsgType", "SIGMET");
	            data.put("score", score);
	            data.put("evalMessage", evalMessage);
	
	            evaluationResultList.add(data);
	
	            if (missingItemList.isEmpty()) {
	                successCount++;
	            } else {
	                failureCount++;
	            }
	        }
	
	        // 평가 결과 저장
	        this.insertEvaluationResultList(evaluationResultList);
	
	        // 트랜잭션 확정
	        this.dbManager.commit();
	
	        long elapsedMillis = System.currentTimeMillis() - startMillis;
	
	        this.printEvaluationResultLog(
	                "SIGMET", startDate, endDate,
	                sigmetDataList.size(), successCount, failureCount, elapsedMillis
	        );
	
	    } catch (Exception e) {
	
	        e.printStackTrace();
	
	        this.dbManager.rollback();
	
	        throw new RuntimeException("EvaluateAviationMessage.evaluateSigmet failed.", e);
	    }
	}


    /**
     * AMIS AIRMET 자료 조회
     */
    private List<Map<String, Object>> getAmisAirmetDataList(String stDtStr, String edDtStr) {
        return this.amisDbmanager.select(this.getAmisAirmetQuery, stDtStr, edDtStr);
    }

    /**
     * AMIS SIGMET 자료 조회
     */
    private List<Map<String, Object>> getAmisSigmetDataList(String stDtStr, String edDtStr) {
        return this.amisDbmanager.select(this.getAmisSigmetQuery, stDtStr, edDtStr);
    }

    /**
     * 누락 항목 메시지 생성
     */
    private String getEvaluationMessage(List<MissingItem> missingItemList) {
    	
        if (missingItemList == null || missingItemList.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (MissingItem item : missingItemList) {
        	
            if (sb.length() > 0) {
                sb.append("\n");
            }

            sb.append(item.getDescription());
        }

        return sb.toString();
    }

    /**
     * 평가 결과 저장
     */
    private void insertEvaluationResultList(List<Map<String, Object>> evaluationResultList) throws SQLException {
    	
        if (evaluationResultList.isEmpty()) {
            return;
        }

        try (PreparedStatement pstmt = this.dbManager.getUnmanagedPrepareStatement(this.insertEvaluationResultQuery)) {
        	
            if (pstmt == null) {
                throw new SQLException("Failed to create evaluation PreparedStatement.");
            }

            int batchCount = 0;

            for (Map<String, Object> data : evaluationResultList) {
                pstmt.setString(1, this.getStringValue(data.get("tm"), 12, "TM"));
                pstmt.setString(2, this.getStringValue(data.get("aviationMsgType"), 6, "AVIATION_MSG_TYPE"));
                pstmt.setString(3, this.getStringValue(data.get("stnCd"), 20, "STN_CD"));
                pstmt.setString(4, this.getStringValue(data.get("msgNo"), 3, "MSG_NO"));
                pstmt.setString(5, this.getStringValue(data.get("msgSts"), 3, "MSG_STS"));
                pstmt.setString(6, this.getStringValue(data.get("mtph"), 50, "MTPH"));
                pstmt.setString(7, this.getTimeString(data.get("stTm")));
                pstmt.setString(8, this.getTimeString(data.get("edTm")));
                pstmt.setString(9, this.getStringValue(data.get("msgSrc"), 1000, "MSG_SRC"));
                pstmt.setString(10, this.getStringValue(data.get("score"), 20, "SCORE"));
                pstmt.setString(11, this.getStringValue(data.get("evalMessage"), 1000, "EVAL_MESSAGE"));

                pstmt.addBatch();
                batchCount++;

                // 500건 단위 Batch 실행
                if (batchCount >= 500) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                    batchCount = 0;
                }
            }

            if (batchCount > 0) {
                pstmt.executeBatch();
                pstmt.clearBatch();
            }
        }
    }

    /**
     * 문자열 변환 및 컬럼 길이 검사
     */
    private String getStringValue(Object value, int maxBytes, String columnName) throws SQLException {
    	
        if (value == null) {
            return null;
        }

        String result = String.valueOf(value);
        int byteLength = result.getBytes(StandardCharsets.UTF_8).length;

        if (byteLength > maxBytes) {
        	
            throw new SQLException(columnName + " exceeds " + maxBytes +" bytes. Actual [" + byteLength + "]");
        }

        return result;
    }

    /**
     * ST_TM / ED_TM 변환
     */
    private String getTimeString(Object value) throws SQLException {
    	
        if (value == null) {
            return null;
        }
        
        String result;
        
        if (value instanceof Date) {
        	
            SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");
            result = sdf.format((Date) value);
        } else {
            result = String.valueOf(value);
        }
        
        if (result.getBytes(StandardCharsets.UTF_8).length > 6) {
        	
            throw new SQLException("Time value exceeds 6 bytes : " + result);
        }

        return result;
    }
    

	/**
	 * 평가 결과 로그 출력
	 */
	private void printEvaluationResultLog(String messageType, Date startDate, Date endDate,
	        int totalCount, int successCount, int failureCount, long elapsedMillis) {
	
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
	    double successRate = totalCount > 0
	            ? successCount * 100.0 / totalCount : 0.0;
	
	    System.out.println("============================================================");
	    System.out.println(" " + messageType + " Evaluation Result");
	    System.out.println("------------------------------------------------------------");
	    System.out.println(" Period       : " + sdf.format(startDate) + " ~ " + sdf.format(endDate));
	    System.out.println(" Total        : " + totalCount);
	    System.out.println(" SUCCESS      : " + successCount);
	    System.out.println(" FAILURE      : " + failureCount);
	    System.out.println(" Success Rate : " + String.format("%.2f", successRate) + "%");
	    System.out.println("------------------------------------------------------------");
	    System.out.println(" Elapsed Time : " + elapsedMillis + " ms");
	    System.out.println("============================================================");
	}


    /**
     * Main
     */

    public static void main(String[] args) {
    	
        EvaluateAviationMessage program = new EvaluateAviationMessage();
        
        try {
            if (!program.initialize()) {
            	
                System.out.println("Error : EvaluateAviationMessage -> initialize failed");
                return;
            }
            
            Calendar cal = new GregorianCalendar();
            Date endDate = cal.getTime();
            
            cal.add(Calendar.DAY_OF_MONTH, -1);
            
            Date startDate = cal.getTime();
            
            program.process(startDate, endDate);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            program.destroy();
        }
    }
}
