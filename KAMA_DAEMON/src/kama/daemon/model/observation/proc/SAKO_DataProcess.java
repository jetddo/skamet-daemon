package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

import java.io.*;
import java.nio.Buffer;
import java.text.MessageFormat;

/**
 * @author chlee
 * Created on 2017-01-10.
 */
public class SAKO_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "sako";
    private static final int DB_COLUMN_COUNT = 9;
    private static final int FILE_DATE_INDEX_POS = 2; // SAKO_ARMY_201611060000_1915
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // TM, STN_ID
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public SAKO_DataProcess(DaemonSettings settings)
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
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        String line;
        String[] token;
        Object[] bindArray;
        int index = 0;
        int[] rawDataPositions = { 45, 2, 3, 4, 7, 13, 14, 23 };
        String query;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            bindArray = new Object[DB_COLUMN_COUNT];

            line = br.readLine();

            // token: #
            token = line.split("#");

            bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameOriginal);

            for (int pos : rawDataPositions)
            {
                bindArray[index++] = token[pos];
            }
        }

        // Record insert example:
        // Remove previous duplicate data to update values
        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
        dbManager.executeUpdate(query);

        // Insert to table
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
        dbManager.executeUpdate(query);
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.ROK_ARMY(TM, STN_ID, WD, WSPD, GUST, VIS, CLA, BASE, TMP, INP_TM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', SYSDATE)");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.ROK_ARMY");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.ROK_ARMY WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}''");
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