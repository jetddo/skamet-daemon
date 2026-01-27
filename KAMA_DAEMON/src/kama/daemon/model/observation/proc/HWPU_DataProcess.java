package kama.daemon.model.observation.proc;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.WPU.WPU_DataProcess_ABS;
import kama.daemon.model.observation.adopt.WPU.WindProfiler;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;

import java.io.*;
import java.text.MessageFormat;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-12-09.
 */
public class HWPU_DataProcess extends WPU_DataProcess_ABS
{
    private static final String DATAFILE_PREFIX = "hwpu";
    private static final int DB_COLUMN_COUNT = 9;
    private static final int FILE_DATE_INDEX_POS = 2; // HWPU_47095_201609010000.DAT
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // TM, WMO_ID, HT
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4;
    private final int INSERT_QUERY_OLD_2 = 5;
    private final int DELETE_QUERY_OLD = 6;

    public HWPU_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    /**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @SuppressWarnings("Duplicates")
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        List<WindProfiler> lstWindProfiler;
        String[] entries;
        String query;

        lstWindProfiler = parseFile(file);

        for (WindProfiler w : lstWindProfiler)
        {
            // Record insert example:
            // Remove previous duplicate data to update values

            entries = convertToRecordFormat(w);

            query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), entries, DB_PRIMARY_KEY_INDEXES);
            dbManager.executeUpdate(query);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[])entries);
            dbManager.executeUpdate(query);
        }

        // 기존 AAMI 데이터 입력
        processOldVersionByFile(dbManager, file);
    }

    @SuppressWarnings("Duplicates")
    //region AAMI 파싱 예전 버전
    private void processOldVersionByFile(DatabaseManager dbManager, File file) throws IOException
    {
        String query;
        String[] fileNameArray = file.getName().split("_");
        Object[] bindArray = new Object[9];

        LineNumberReader rdr = new LineNumberReader(new BufferedReader(new FileReader(file)));

        String[] strLines = new String[2];

        for (String line; (line = rdr.readLine()) != null;)
        {
            if (rdr.getLineNumber() >= 3)
            {
                break;
            }
            strLines[rdr.getLineNumber() - 1] = line;
        }

        rdr.close();

        String[] rawData = strLines[0].split("#");

        bindArray[0] = rawData[1] + rawData[2]; // WMO_ID
        bindArray[1] = rawData[9]; // LAT
        bindArray[2] = rawData[8]; // LON
        bindArray[3] = rawData[10]; // H_HT
        bindArray[4] = fileNameArray[2].split("\\.")[0] + "#" + strLines[1]; // WIND
        bindArray[5] = fileNameArray[2].split("\\.")[0]; // DT

        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY_OLD), bindArray, DB_PRIMARY_KEY_INDEXES);
        dbManager.executeUpdate(query);

        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);

        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);
    }
    //endregion

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.HWPU(TM, WMO_ID, HT, LAT, LON, TD, WSPD, DATA_STT, IDX_SEQ) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.HWPU WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND WMO_ID = ''{1}'' AND HT = ''{2}''");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.HWPU(WMO_ID, LAT, LON, H_HT, WIND, DT) VALUES  (''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', TO_DATE(''{5}'', ''YYYYMMDDHH24mi''))");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.HWPU_B(WMO_ID, LAT, LON, H_HT, WIND, DT) VALUES  (''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', TO_DATE(''{5}'', ''YYYYMMDDHH24mi''))");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.HWPU WHERE WMO_ID=''{0}''");
    }
    //</editor-fold>

    //<editor-fold desc="Auto-generated getters (No need to modify)">
    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
    //</editor-fold>
}