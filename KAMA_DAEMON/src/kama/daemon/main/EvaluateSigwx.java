
package kama.daemon.main;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

/**
 * SIGWX 적시성 평가 프로그램
 *
 * @author jetddo
 * Created on 2017-01-03
 */
public class EvaluateSigwx {

    private SimpleDateFormat logDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final String sigwxRootPath = "/amis_data/sigwx_data/sigwx_work_root";

    private final String mergeEvaluationResultQuery =
            "MERGE INTO AAMI.EVAL_SIGWX A " +
            "USING ( " +
            "    SELECT TO_DATE(?, 'YYYYMMDDHH24MI') AS TM, " +
            "           ? AS SIGWX_TYPE, " +
            "           ? AS SIGWX_FILE_NAME, " +
            "           TO_DATE(?, 'YYYYMMDDHH24MISS') AS SEND_TM, " +
            "           ? AS TM_DIFF, " +
            "           ? AS SCORE, " +
            "           ? AS EVAL_MESSAGE " +
            "    FROM DUAL " +
            ") B " +
            "ON (A.TM = B.TM AND A.SIGWX_TYPE = B.SIGWX_TYPE) " +
            "WHEN MATCHED THEN UPDATE SET " +
            "    A.SIGWX_FILE_NAME = COALESCE(B.SIGWX_FILE_NAME, A.SIGWX_FILE_NAME), " +
            "    A.SEND_TM = COALESCE(B.SEND_TM, A.SEND_TM), " +
            "    A.TM_DIFF = COALESCE(B.TM_DIFF, A.TM_DIFF), " +
            "    A.SCORE = CASE " +
            "        WHEN B.SEND_TM IS NULL AND A.SEND_TM IS NOT NULL " +
            "        THEN A.SCORE ELSE B.SCORE END, " +
            "    A.EVAL_MESSAGE = CASE " +
            "        WHEN B.SEND_TM IS NULL AND A.SEND_TM IS NOT NULL " +
            "        THEN A.EVAL_MESSAGE ELSE B.EVAL_MESSAGE END " +
            "WHEN NOT MATCHED THEN INSERT ( " +
            "    TM, SIGWX_TYPE, SIGWX_FILE_NAME, SEND_TM, " +
            "    TM_DIFF, SCORE, EVAL_MESSAGE " +
            ") VALUES ( " +
            "    B.TM, B.SIGWX_TYPE, B.SIGWX_FILE_NAME, B.SEND_TM, " +
            "    B.TM_DIFF, B.SCORE, B.EVAL_MESSAGE " +
            ")";

    private final int[] issueHours = {5, 11, 17, 23};

    private final String[] sigwxTypes = {"LOW", "MID"};

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final DateTimeFormatter sendTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private Configuration config;

    private DatabaseManager dbManager;

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

            return true;

        } catch (Exception e) {
            System.out.println("Error : EvaluateSigwx.initialize -> " + e);
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
    }

    /**
     * 평가 실행
     */
    private void process(Date startDate, Date endDate) {

        System.out.println(this.logDateFormat.format(new Date()) +" -> ::::: Start EvaluateSigwx :::::");

        try {
        	
            this.evaluateSigwx(startDate, endDate);

        } catch (Exception e) {
        	
            System.out.println("Error : EvaluateSigwx.process -> " + e);
            e.printStackTrace();
        }

        System.out.println(this.logDateFormat.format(new Date()) +" -> ::::: End EvaluateSigwx :::::"
        );
    }

	/**
	 * SIGWX 평가
	 */
	private void evaluateSigwx(Date startDate, Date endDate) {
	
	    List<Map<String, Object>> evaluationResultList = new ArrayList<Map<String, Object>>();
	
	    long startMillis = System.currentTimeMillis();
	
	    int normalCount = 0;
	    int delayCount = 0;
	    int missingCount = 0;
	
	    Map<String, int[]> typeStats = new HashMap<String, int[]>();
	
	    for (String sigwxType : this.sigwxTypes) {
	        typeStats.put(sigwxType, new int[3]);
	    }
	
	    try {
	
	        String sigwxDeployPath = this.sigwxRootPath + File.separator + "deploy_files";
	
	        // 입력 Date는 UTC 기준 날짜 범위로 해석
	        LocalDate start = Instant.ofEpochMilli(startDate.getTime()).atOffset(ZoneOffset.UTC).toLocalDate();
	        LocalDate end = Instant.ofEpochMilli(endDate.getTime()).atOffset(ZoneOffset.UTC).toLocalDate();
	
	        Instant now = Instant.now();
	
	        // 종료일은 미포함
	        for (LocalDate date = start; date.isBefore(end); date = date.plusDays(1)) {
	
	            String directoryPath = sigwxDeployPath +
	                    File.separator + date.getYear() +
	                    File.separator + String.format("%02d", date.getMonthValue()) +
	                    File.separator + String.format("%02d", date.getDayOfMonth());
	
	            File directory = new File(directoryPath);
	
	            for (int issueHour : this.issueHours) {
	
	                LocalDateTime issueTime = date.atTime(issueHour, 0);
	                Instant issueInstant = issueTime.toInstant(ZoneOffset.UTC);
	
	                // 아직 발표시각이 도래하지 않은 자료는 제외
	                if (issueInstant.isAfter(now)) {
	                    continue;
	                }
	
	                for (String sigwxType : this.sigwxTypes) {
	
	                    Map<String, Object> result = this.evaluateSigwxFile(
	                            directory, issueTime, sigwxType
	                    );
	
	                    evaluationResultList.add(result);
	
	                    String evalMessage = String.valueOf(result.get("evalMessage"));
	                    int[] stats = typeStats.get(sigwxType);
	
	                    if ("NORMAL".equals(evalMessage)) {
	                        normalCount++;
	                        stats[0]++;
	
	                    } else if ("DELAY".equals(evalMessage)) {
	                        delayCount++;
	                        stats[1]++;
	
	                    } else if ("MISSING".equals(evalMessage)) {
	                        missingCount++;
	                        stats[2]++;
	                    }
	                }
	            }
	        }
	
	        this.insertEvaluationResultList(evaluationResultList);
	
	        this.dbManager.commit();
	
	        int totalCount = evaluationResultList.size();
	
	        double normalRate = totalCount > 0
	                ? normalCount * 100.0 / totalCount : 0.0;
	
	        long elapsedMillis = System.currentTimeMillis() - startMillis;
	
	        System.out.println("============================================================");
	        System.out.println(" SIGWX Evaluation Result");
	        System.out.println("------------------------------------------------------------");
	        System.out.println(" Period (UTC) : " + start + " ~ " + end + " (End Exclusive)");
	        System.out.println(" Total        : " + totalCount);
	        System.out.println(" NORMAL       : " + normalCount);
	        System.out.println(" DELAY        : " + delayCount);
	        System.out.println(" MISSING      : " + missingCount);
	        System.out.println(" Normal Rate  : " + String.format("%.2f", normalRate) + "%");
	        System.out.println("------------------------------------------------------------");
	        System.out.println(" TYPE       TOTAL     NORMAL     DELAY     MISSING");
	
	        for (String sigwxType : this.sigwxTypes) {
	
	            int[] stats = typeStats.get(sigwxType);
	            int typeTotal = stats[0] + stats[1] + stats[2];
	
	            System.out.println(String.format(
	                    " %-10s %-9d %-10d %-9d %d",
	                    sigwxType, typeTotal, stats[0], stats[1], stats[2]
	            ));
	        }
	
	        System.out.println("------------------------------------------------------------");
	        System.out.println(" Elapsed Time : " + elapsedMillis + " ms");
	        System.out.println("============================================================");
	
	    } catch (Exception e) {
	
	        e.printStackTrace();
	
	        this.dbManager.rollback();
	
	        throw new RuntimeException("EvaluateSigwx.evaluateSigwx failed.", e);
	    }
	}


    /**
     * 개별 SIGWX 파일 평가
     */
    private Map<String, Object> evaluateSigwxFile(File directory, LocalDateTime issueTime, String sigwxType) throws Exception {

        Map<String, Object> result = new HashMap<String, Object>();

        String fileName = String.format(
                "SIGWX_%s_%04d%02d%02d%02d.png",
                sigwxType,
                issueTime.getYear(),
                issueTime.getMonthValue(),
                issueTime.getDayOfMonth(),
                issueTime.getHour()
        );

        File file = new File(directory, fileName);

        result.put("tm", issueTime.format(this.timeFormatter));
        result.put("sigwxType", sigwxType);

        if (!file.isFile()) {

            result.put("sigwxFileName", null);
            result.put("sendTm", null);
            result.put("tmDiff", null);
            result.put("score", 0);
            result.put("evalMessage", "MISSING");

            return result;
        }

        // 실제 파일 생성시각 조회

        Instant sendInstant = Files.getLastModifiedTime(file.toPath()).toInstant();

        Instant issueInstant = issueTime.toInstant(ZoneOffset.UTC);

        // 초 단위까지 비교
        long diffSeconds = Duration.between(issueInstant, sendInstant).getSeconds();

        // 분 단위 저장 (양수 지연은 올림 처리)
        long diffMinutes = diffSeconds > 0 ? (diffSeconds + 59) / 60 : Math.floorDiv(diffSeconds, 60);

        LocalDateTime sendTime = LocalDateTime.ofInstant(sendInstant, ZoneOffset.UTC);

        boolean delayed = sendInstant.isAfter(issueInstant);

        result.put("sigwxFileName", fileName);
        result.put("sendTm", sendTime.format(this.sendTimeFormatter));
        result.put("tmDiff", diffMinutes);
        result.put("score", delayed ? 0 : 100);
        result.put("evalMessage", delayed ? "DELAY" : "NORMAL");

        return result;
    }

    /**
     * 평가 결과 저장
     */
    private void insertEvaluationResultList(List<Map<String, Object>> evaluationResultList) throws SQLException {

        if (evaluationResultList.isEmpty()) {
            return;
        }

        try (PreparedStatement pstmt = this.dbManager.getUnmanagedPrepareStatement(this.mergeEvaluationResultQuery)) {

            if (pstmt == null) {
                throw new SQLException("Failed to create evaluation PreparedStatement.");
            }

            int batchCount = 0;

            for (Map<String, Object> data : evaluationResultList) {

                pstmt.setString(1, this.getStringValue(data.get("tm"), 12, "TM"));

                pstmt.setString(2, this.getStringValue(data.get("sigwxType"), 3, "SIGWX_TYPE"));

                pstmt.setString(3, this.getStringValue(data.get("sigwxFileName"), 32, "SIGWX_FILE_NAME"));

                pstmt.setString(4, this.getStringValue(data.get("sendTm"), 14, "SEND_TM"));

                Object tmDiff = data.get("tmDiff");

                if (tmDiff == null) {
                    pstmt.setNull(5, java.sql.Types.NUMERIC);
                } else {
                    pstmt.setLong(5, ((Number) tmDiff).longValue());
                }

                pstmt.setInt(6, ((Number) data.get("score")).intValue());

                pstmt.setString(7, this.getStringValue(data.get("evalMessage"), 32, "EVAL_MESSAGE"));

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
    private String getStringValue(
            Object value,
            int maxBytes,
            String columnName) throws SQLException {

        if (value == null) {
            return null;
        }

        String result = String.valueOf(value);

        int byteLength =
                result.getBytes(StandardCharsets.UTF_8).length;

        if (byteLength > maxBytes) {

            throw new SQLException(
                    columnName + " exceeds " + maxBytes +
                    " bytes. Actual [" + byteLength + "]"
            );
        }

        return result;
    }

    /**
     * Main
     */
    public static void main(String[] args) {

        EvaluateSigwx program = new EvaluateSigwx();

        try {
            if (!program.initialize()) {

                System.out.println("Error : EvaluateSigwx -> initialize failed");
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
