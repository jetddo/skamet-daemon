package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class WPF_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "wpf";
    private static final int DB_COLUMN_COUNT = 11;
    private static final int FILE_DATE_INDEX_POS = 4; // WPF_DATA_MIN_RKJY_20160120160949.txt
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3 }; // TM, STN_ID, RWY_USE, ALTITUDE
    private static final int[] DB_PRIMARY_KEY_INDEXES_OLD = {1}; // STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4; // backward compatibility (AAMI)
    private final int INSERT_QUERY_OLD_2 = 5; // backward compatibility (AAMI)
    private final int DELETE_QUERY_OLD = 6;

    public WPF_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.WPF_DATA(TM, STN_ID, RWY_USE, ALTITUDE, RWY_CD, WD, WSPD, UU, VV, WW, DATA_ST) " +
                "VALUES (TO_DATE(''{0}'',''YYYYMMDDHH24mi''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', " +
                "''{7}'', ''{8}'', ''{9}'', ''{10}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.WPF_DATA WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}'' AND RWY_USE = ''{2}'' AND ALTITUDE = ''{3}''");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.WPF(TM, STN_ID, RWY_DIR, FT, WD, WS, NUM, OBS_TM) VALUES  (TO_DATE(''{0}'',''YYYYMMDDHH24mi''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYY-MM-DD HH24:mi:ss''))");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.WPF_B(TM, STN_ID, RWY_DIR, FT, WD, WS, NUM, OBS_TM) VALUES  (TO_DATE(''{0}'',''YYYYMMDDHH24mi''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYY-MM-DD HH24:mi:ss''))");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.WPF WHERE STN_ID=''{0}''");
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
        String query = null;
        Object[] bindArray = new Object[DB_COLUMN_COUNT];
        Arrays.fill(bindArray, "");

        int lineNumber;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            lineNumber = 0;

            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;

                rawData = line.split(",");

                // Insert data to each column
                for (int r = 0; r < bindArray.length; r++)
                {
                    bindArray[r] = rawData[r];
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // WPF 테이블 입력
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);

                processOldVersionByLine(dbManager, line, lineNumber++, processorInfo.FileDateFromNameOriginal);
            }
        }
    }

    private void processOldVersionByLine(DatabaseManager dbManager, String line, int lineNumber, Date fileDate)
    {
        String query;
        Object[] bindArray = new Object[8];
        String[] rawData = line.split(",");

        bindArray[0] = rawData[0]; // TM
        bindArray[1] = rawData[1]; // STN_ID
        bindArray[2] = rawData[2]; // RWY_DIR
        bindArray[3] = rawData[3]; // FT
        bindArray[4] = rawData[5]; // WD
        bindArray[5] = rawData[6]; // WS
        bindArray[6] = String.valueOf(lineNumber); // NUM
        bindArray[7] = convertToDBText(fileDate); // OBS_TM

        // WPF 테이블 삭제
        if (lineNumber == 0)
        {
            query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY_OLD), bindArray, DB_PRIMARY_KEY_INDEXES_OLD);
            dbManager.executeUpdate(query);
        }

        // WPF 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);

        // WPF_B 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}