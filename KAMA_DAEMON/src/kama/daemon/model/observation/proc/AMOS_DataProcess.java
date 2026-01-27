package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.AMOS.AMOS_Dict;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Arrays;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class AMOS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "amos";
    private static final int DB_COLUMN_COUNT = 71;
    private static final int FILE_DATE_INDEX_POS = 3; // AMOS_MIN_110_20160120160900
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // TM, STN_ID, RWY_DIR
    private static final int[] DB_PRIMARY_KEY_INDEXES_OLD = { 1 }; // STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4;
    private final int INSERT_QUERY_OLD_2 = 5;
    private final int MERGE_QUERY_OLD = 6;
    private final int DELETE_QUERY_OLD = 7;

    public AMOS_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.AMOS(TM, STN_ID, RWY_DIR, RWY_USE, WD_3SEC_AVG, WD_1MIN_AVG, WD_1MIN_MNM, WD_1MIN_MAX, WD_2MIN_AVG, WD_2MIN_MNM, WD_2MIN_MAX, WD_10MIN_AVG, WD_10MIN_MNM, WD_10MIN_MAX, WSPD_3SEC_AVG, WSPD_1MIN_AVG, WSPD_1MIN_MNM, WSPD_1MIN_MAX, WSPD_2MIN_AVG, WSPD_2MIN_MNM, WSPD_2MIN_MAX, WSPD_10MIN_AVG, WSPD_10MIN_MNM, WSPD_10MIN_MAX, WSPD_3SEC_MAX, GUST_2MIN, GUST_10MIN, TAIL_2MIN, TAIL_10MIN, CRW_2MIN, CRW_10MIN, MOR_1MIN, MOR_1MIN_MID, MOR_10MIN, MOR_10MIN_MID, MOR_10MIN_MNM, MOR_10MIN_MAX, RVR_1MIN, RVR_1MIN_MID, RVR_10MIN, RVR_10MIN_MID, RVR_10MIN_MNM, RVR_10MIN_MAX, LGT_INTST_EG, LGT_INTST_C, BCL, WW_CO, WW_LTTR, VIS_1MIN, VIS_10MIN, CLA_1LYR, CLA_2LYR, CLA_3LYR, BASE_1LYR, BASE_2LYR, BASE_3LYR, VER_VIS, TMP, DP, HM, RN_1MIN, RN_30MIN, RN_1HR, RN_3HR, RN_12HR, RN_1DD, PRSS, QFE, QNH, QFF, CAL_RN_1DD) VALUES  (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'', ''{20}'', ''{21}'', ''{22}'', ''{23}'', ''{24}'', ''{25}'', ''{26}'', ''{27}'', ''{28}'', ''{29}'', ''{30}'', ''{31}'', ''{32}'', ''{33}'', ''{34}'', ''{35}'', ''{36}'', ''{37}'', ''{38}'', ''{39}'', ''{40}'', ''{41}'', ''{42}'', ''{43}'', ''{44}'', ''{45}'', ''{46}'', ''{47}'', ''{48}'', ''{49}'', ''{50}'', ''{51}'', ''{52}'', ''{53}'', ''{54}'', ''{55}'', ''{56}'', ''{57}'', ''{58}'', ''{59}'', ''{60}'', ''{61}'', ''{62}'', ''{63}'', ''{64}'', ''{65}'', ''{66}'', ''{67}'', ''{68}'', ''{69}'', ''{70}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.AMOS WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}'' AND RWY_DIR = ''{2}''");

        // 기존 AAMI 데이터 입력
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AMOS(TM, STN_ID, RWY_DIR, RWY_USE, WD10M_AVG, WD10M_MIN, WD10M_MAX, WS10M_AVG, WS10M_MIN, WS10M_MAX, MOR, RVR, TA, HUM, RWY_DEG, VISIBILITY) VALUES  (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AMOS_B(TM, STN_ID, RWY_DIR, RWY_USE, WD10M_AVG, WD10M_MIN, WD10M_MAX, WS10M_AVG, WS10M_MIN, WS10M_MAX, MOR, RVR, TA, HUM, RWY_DEG, VISIBILITY) VALUES  (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'')");
        defineQueryFormat(MERGE_QUERY_OLD, "MERGE INTO %%WORKSPACE_OLD_AAMI%%.AIR_RWY a USING DUAL b ON (a.STN_ID=''{1}'' AND a.RWY_DIR=''{2}'') WHEN MATCHED THEN UPDATE SET " + "a.RWY_USE=''{3}'', a.RWY_DEG=''{14}'' WHEN NOT MATCHED THEN INSERT " + "(a.STN_ID, a.RWY_DIR, a.RWY_USE, a.RWY_DEG) VALUES  (''{1}'', ''{2}'', ''{3}'', ''{14}'')");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.AMOS WHERE STN_ID=''{0}''");
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

        // AMOS 테이블 삭제
        //db.executeUpdate(MessageFormat.format(this.deleteQueryFormat, bindArray));

        AMOS_Dict dict = AMOS_Dict.getInstance();

        String stationID;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            stationID = file.getName().split("_")[2];

            // truncate table (old AAMI)
            query = MessageFormat.format(retrieveQueryFormat(DELETE_QUERY_OLD), stationID);
            dbManager.executeUpdate(query);

            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;
                int index;

                rawData = line.split("#");

                // Parse date from raw data file
                try
                {
                    String sDateTime;
                    sDateTime = MessageFormat.format("{0} {1}", rawData[0], rawData[1]);
                    bindArray[0] = DateFormatter.changeFormat(sDateTime, "yyyy-MM-dd HH:mm:ss", "yyyyMMddHHmmss");
                } catch (ParseException pe)
                {
                    throw new DaemonException("Error : unable to parse date");
                }

                // Add value according to the hashmap
                for (int i = 0; i < 37/*rawData.length*/; i++)
                {
                    index = dict.getIndexFromResToOracle(i);
                    bindArray[index] = rawData[i];
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // Insert data to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);

                // 기존 AAMI 데이터 입력
                processOldVersionByLine(dbManager, stationID, line);
            }
        }
    }

    private void processOldVersionByLine(DatabaseManager dbManager, String STN_ID, String line)
    {
        String query;
        Object[] bindArray = new Object[16];
        String[] rawData = line.split("#");

        bindArray[1] = STN_ID; // STN_ID

        bindArray[0] = rawData[0]; // TM
        bindArray[2] = rawData[2]; // RWY_DIR
        bindArray[3] = rawData[3]; // RWY_USE
        bindArray[4] = rawData[7]; // WD10M_AVG
        bindArray[5] = rawData[8]; // WD10M_MIN
        bindArray[6] = rawData[9]; // WD10M_MAX
        bindArray[7] = (Double.parseDouble(rawData[13])) * 0.1; // WS10M_AVG
        bindArray[8] = (Double.parseDouble(rawData[14])) * 0.1; // WS10M_MIN
        bindArray[9] = (Double.parseDouble(rawData[15])) * 0.1; // WS10M_MAX
        bindArray[10] = rawData[16]; // MOR
        bindArray[11] = rawData[20]; // RVR
        bindArray[12] = (Float.parseFloat(rawData[30])) * 0.1; // TA
        bindArray[13] = (Float.parseFloat(rawData[32])) * 0.1; // HUM
        bindArray[14] = rawData[2].length() >= 3 ? Integer.parseInt(rawData[2].substring(0, 2)) * 10
                : Integer.parseInt(rawData[2]) * 10; // RWY_DEG
        bindArray[15] = rawData[26]; // VISIBILITY

        // AMOS 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);

        // AMOS_B 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);

        // AIR_RWY 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(MERGE_QUERY_OLD), bindArray);
        dbManager.executeUpdate(query);
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}