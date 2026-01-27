package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class AWS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "aws";
    private static final int DB_COLUMN_COUNT = 18;
    private static final int FILE_DATE_INDEX_POS = 2; // AWS_MIN_201605201609
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // TM, STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4; // backward compatibility (AAMI)
    private final int INSERT_QUERY_OLD_2 = 5; // backward compatibility (AAMI)
    private final int DELETE_QUERY_OLD = 6;

    public AWS_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.KMA_AWS_1MIN(TM, STN_ID, LAT, LON, HT, WD, WS, TA, HM, PA, PS, RN_YN, RN_1HR, RN_DAY, RN_15M, RN_60M, WD_INS, WS_INS) VALUES  (TO_DATE(''{1}'', ''YYYYMMDDHH24miss''), ''{0}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.KMA_AWS_1MIN WHERE TM = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{0}''");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AWS(STN_ID, TM, LAT, LON, HT, WD, WS, TA, HM) VALUES  (''{0}'', TO_DATE(''{1}'', ''YYYYMMDDHH24mi''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AWS_B(STN_ID, TM, LAT, LON, HT, WD, WS, TA, HM) VALUES  (''{0}'', TO_DATE(''{1}'', ''YYYYMMDDHH24mi''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.AWS");
    }

    /**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        String query;
        Object[] bindArray;

        query = null;
        bindArray = new Object[DB_COLUMN_COUNT];
        Arrays.fill(bindArray, "");

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            // AAMI.AWS 테이블 내 기존 데이터 삭제
            query = retrieveQueryFormat(DELETE_QUERY_OLD);
            dbManager.executeUpdate(query);

            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;
                rawData = line.split("#");

                // Add value according to the raw data columns
                for (int i = 0; i < bindArray.length; i++)
                {
                    if (rawData[i] != null)
                    {
                        bindArray[i] = rawData[i];
                    }
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // KMA 테이블 입력
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);

                // 기존 AAMI 데이터 입력
                processOldVersionByLine(dbManager, line);
            }
        }
    }

    //region AAMI 파싱 예전 버전
    private void processOldVersionByLine(DatabaseManager dbManager, String line)
    {
        String query;
        Object[] bindArray = new Object[9];
        String[] rawData = line.split("#");

        bindArray[0] = rawData[0]; // STN_ID
        bindArray[1] = rawData[1]; // TM
        bindArray[2] = rawData[2]; // LAT
        bindArray[3] = rawData[3]; // LON
        bindArray[4] = rawData[4]; // HT
        bindArray[5] = rawData[5]; // WD
        bindArray[6] = rawData[6]; // WS
        bindArray[7] = rawData[7]; // TA
        bindArray[8] = rawData[8]; // HM

        // 기존 AAMI AWS 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);

        // 기존 AAMI AWS_B 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);
    }
    //endregion

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}