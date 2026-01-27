package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.Log;
import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chlee on 2017-02-15.
 */
public class UM_VIS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "um_vis";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/visibility.nc
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;

    public UM_VIS_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override@SuppressWarnings("Duplicates")
    protected void processDataByFileGroup(DatabaseManager dbManager, File[] dataFiles, ProcessorInfo processorInfo) throws Exception
    {
        String query = null;
        Object[] bindArray = new Object[2];
        List<String> queriesList;

        queriesList = new ArrayList<>();

        // 파일 전부 extract
        for (File file : dataFiles)
        {
            // 처리할 파일명 로그 print
        	Log.print("INFO : File NAME -> {0}", file.getAbsoluteFile());

            // AAMI 테이블 입력
            bindArray[0] = convertToDBText(processorInfo.FileDateFromNameOriginal);
            bindArray[1] = convertToDBText(file.getName());

            // UM_VIS 테이블 입력
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_1), bindArray);
            queriesList.add(query);

            // UM_VIS_B 테이블 입력
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_2), bindArray);
            queriesList.add(query);

            if (DataFileStore.storeDateFile(file, processorInfo.FileSavePath))
            {
                query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, file.getName()));
                queriesList.add(query);
            }
        }

        for (File file : dataFiles)
        {
            if (file.exists())
            {
                file.delete();
            }
        }

        // UM_VIS 테이블 데이터 삭제
        query = retrieveQueryFormat(DELETE_QUERY);
        dbManager.executeUpdate(query);

        // 쿼리 한꺼번에 처리
        for (String savedQuery : queriesList)
        {
            dbManager.executeUpdate(savedQuery);
        }

        dbManager.commit();
    }

    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        throw new NotImplementedException("Not implemented");
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        // ******* 기존 AAMI 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_1, "INSERT INTO %%WORKSPACE%%.NMDL_UM_VIS(FILE_DT, FILE_NM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(INSERT_QUERY_2, "INSERT INTO %%WORKSPACE%%.NMDL_UM_VIS_B(FILE_DT, FILE_NM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_UM_VIS");
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
