package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.*;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.compress.*;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chlee on 2017-02-15.
 */
public class UM_LOA_PC_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "um_loa_pc";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/qwumloa_pb000.nc.tgz
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;

    public UM_LOA_PC_DataProcess(DaemonSettings settings)
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

        // subgroup 갯수
        Log.print("INFO : File subgroup COUNT -> {0}", dataFiles.length);

        // 데이터파일이 19개가 모여야 처리가 가능함.
//        if (dataFiles.length < 19)
//        {
//            return;
//        }

        // 파일 전부 extract
        for (File file : dataFiles)
        {
            // 처리할 파일명 로그 print
        	Log.print("INFO : File NAME -> {0}", file.getAbsoluteFile());

            GZipTgzReader reader;
            String fullFile = file.getAbsolutePath();
            File[] extractedFiles;
            File baseDir = file.getParentFile();

            reader = new GZipTgzReader(fullFile);

            extractedFiles = reader.extractTgzToDirectory(baseDir);

            // 압축 내 nc 파일이 하나 이상일 경우 대비
            for (File eFile : extractedFiles)
            {
                Log.print("INFO : Extracted file -> {0}", eFile.getName());

                if (eFile.getName().endsWith(".nc"))
                {
                 
                    if (DataFileStore.storeDateFile(eFile, processorInfo.FileSavePath))
                    {
                        query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, eFile.getName()));
                        queriesList.add(query);
                    }
                }
            }
        }

        // 쿼리 한꺼번에 처리
        for (String savedQuery : queriesList)
        {
            dbManager.executeUpdate(savedQuery);
        }

        for (File file : dataFiles)
        {
            if (file.exists())
            {
                file.delete();
            }
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
