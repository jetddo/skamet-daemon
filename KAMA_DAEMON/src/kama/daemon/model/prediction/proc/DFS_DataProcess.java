package kama.daemon.model.prediction.proc;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.prediction.adopt.DFS.DFS_Type;
import kama.daemon.model.prediction.adopt.DFS.loader.DFS_GRB1_Callback;
import kama.daemon.model.prediction.adopt.DFS.loader.DFS_GRB1_Constants;
import kama.daemon.model.prediction.adopt.DFS.loader.DFS_GRB1_Loader;
import kama.daemon.model.prediction.adopt.DFS.loader.section.DFS_GRB1_INF;

public class DFS_DataProcess extends DataProcessor {

	// 동네예보 데이터 파일명 Prefix
	private static final String DATAFILE_PREFIX = "dfs";

	// 동네예보 데이터 파일명을 '.' 문자로 분리하였을 때 날짜가 위치한 인덱스 번호
	private static final int DATAFILE_DATE_INDEX_POS = 1;

	private final int DELETE_QUERY_ID = 1;

	public DFS_DataProcess(final DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
	}

	@Override
	protected void processDataInternal(final DatabaseManager dbManager, final File file, final ProcessorInfo processorInfo) throws Exception {
		final DFS_Type dfsType = getType(file);

		final SimpleDateFormat forecastTmFormat = new SimpleDateFormat("yyyyMMddHHmmss");

		(new DFS_GRB1_Loader()).parse(file, new DFS_GRB1_Callback() {
			@Override
			public boolean callback(final DFS_GRB1_INF inf, final float[][] dfsData) {
            	String columnName = dfsType.name();

		        // 동네예보 발표시각(UTC)을 구한다.
            	String tm = String.format("%04d%02d%02d%02d%02d%02d", inf.s1.YY, inf.s1.MM, inf.s1.DD, inf.s1.HH, inf.s1.MI, 0);

            	// 동네예보 예보시각(UTC)을 구한다.
            	Calendar cal = Calendar.getInstance();
            	cal.set(inf.s1.YY, inf.s1.MM - 1, inf.s1.DD, inf.s1.HH, inf.s1.MI, 0);
            	cal.add(Calendar.HOUR, inf.s1.P1);
            	String forecastTm = forecastTmFormat.format(cal.getTime());

            	// SQL문을 생성한다.
				StringBuilder sbSQL = new StringBuilder()
					.append("   MERGE INTO %%WORKSPACE%%.DFS                                                              ")
		            .append("              USING DUAL                                                                     ")
		            .append("       ON (                                                                                  ")
		            .append("               TM = TO_DATE(?, 'YYYYMMDDHH24MISS')                                           ")
		            .append("           AND FORECAST_TM = TO_DATE(?, 'YYYYMMDDHH24MISS')                                  ")
		            .append("           AND X = ?                                                                         ")
		            .append("           AND Y = ?                                                                         ")
		            .append("       )                                                                                     ")
		            .append("       WHEN MATCHED THEN                                                                     ")
		            .append("           UPDATE                                                                            ")
		            .append("              SET ").append(columnName).append(" = ?                                         ")
		            .append("       WHEN NOT MATCHED THEN                                                                 ")
		            .append("           INSERT                                                                            ")
		            .append("               ( TM, FORECAST_TM, X, Y, ").append(columnName).append(" )                     ")
		            .append("           VALUES                                                                            ")
		            .append("               (                                                                             ")
		            .append("                   TO_DATE(?, 'YYYYMMDDHH24MISS'),                                           ")
		            .append("                   TO_DATE(?, 'YYYYMMDDHH24MISS'),                                           ")
		            .append("                   ?,                                                                        ")
		            .append("                   ?,                                                                        ")
		            .append("                   ?                                                                         ")
		            .append("               )                                                                             ");

				String sql = makeQuery(sbSQL.toString());

				
				/**
				 * 동네예보 데이터를 DB에 추가한다.
				 */
				PreparedStatement pstmt = null;

				try {
					pstmt = dbManager.getUnmanagedPrepareStatement(sql);

					int addedBatchCount = 0;
			        for (int j1 = 0; j1 < DFS_GRB1_Constants.NY; ++j1) {
			            for (int i1 = 0; i1 < DFS_GRB1_Constants.NX; ++i1) {
							pstmt.setString(1, tm);
							pstmt.setString(2, forecastTm);
							pstmt.setInt(3, i1 + 1);
							pstmt.setInt(4, j1 + 1);
			            	pstmt.setFloat(5, dfsData[j1][i1]);
			            	pstmt.setString(6, tm);
							pstmt.setString(7, forecastTm);
							pstmt.setInt(8, i1 + 1);
							pstmt.setInt(9, j1 + 1);
			            	pstmt.setFloat(10, dfsData[j1][i1]);

			            	// 배치를 추가한다.
							pstmt.addBatch();

							pstmt.clearParameters();

							++addedBatchCount;

							// 메모리 부족을 고려하여 커밋한다.
							if ((addedBatchCount % 3000) == 0) {
								pstmt.executeBatch();
								pstmt.clearBatch();

								dbManager.commit(false);

								addedBatchCount = 0;
							}
			            }
			        }

			        // 아직 커밋되지 않은 Batch를 커밋한다.
			        if (addedBatchCount > 0) {
				        pstmt.executeBatch();
				        pstmt.clearBatch();

						dbManager.commit(false);
			        }
				} catch (final SQLException e) {
					dbManager.rollback();

					return false;
				} finally {
					if (pstmt != null) {
						try {
							pstmt.close();
						} catch (SQLException e) {
						}
					}
				}

				return true;
			}
		});

		// 동네예보 파일 처리 이력에 처리 완료된 동네예보 요소 정보를 추가한다.
		dbManager.executeUpdate(buildCompletedFileProcessQuery(processorInfo, dfsType));

		// 지정된 일자 이전의 동네예보 데이터를 삭제한다.
		dbManager.executeUpdate(retrieveQueryFormat(DELETE_QUERY_ID));

		dbManager.commit();
	}

	@Override
	protected Date parseDateFromFileName(final File file) throws ParseException {
		final int dateFileIndex = getDateFileIndex();
		final String dateString = file.getName().split("\\.")[dateFileIndex];

		return DateFormatter.parseDate(dateString, "yyyyMMddHHmm");
	}

	@Override
	protected void defineQueries() {
		defineQueryFormat(DELETE_QUERY_ID, "DELETE FROM %%WORKSPACE%%.DFS WHERE TM < ( SELECT SYSDATE - 4 FROM DUAL )");
	}

	@Override
	protected int getDateFileIndex() {
		return DATAFILE_DATE_INDEX_POS;
	}

    protected String buildCompletedFileProcessQuery(final ProcessorInfo processorInfo, final DFS_Type dfsType) {
		return new StringBuilder()
			.append("INSERT INTO AAMI.DFS_FILE_PROC_H ( TM, TY_CD, PROC_DT )")
			.append("     VALUES (")
			.append("                 TO_DATE('").append(convertToDBText(processorInfo.FileDateFromNameOriginal)).append("', 'YYYY-MM-DD HH24:mi:ss'),")
			.append("                 '").append(dfsType.name()).append("',")
			.append("                 SYSDATE")
			.append("            )")
			.toString();
    }

    protected DFS_Type getType(final File file) {
    	// 동네예보 파일명(DFS_SHRT_GRD_GRB3_xxx.201710102300)에서 '.' 이후의 날짜 문자열을 제거한 파일명을 구한다.
    	String fileName = file.getName().split("\\.")[0];

    	return DFS_Type.valueOf(fileName.split("_")[4]);
    }

}
