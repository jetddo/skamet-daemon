package kama.daemon.model.prediction.proc;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.Log;

/**
 * Created by chlee on 2017-02-15.
 */
public class IGTCHT_IMG_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "igtcht_img";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/qwumrea_pb000.nc.tgz
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;

    public IGTCHT_IMG_DataProcess(DaemonSettings settings)
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
        
        List<File> pngFileList = new ArrayList<File>();

        // subgroup 갯수
        Log.print("INFO : File subgroup COUNT -> {0}", dataFiles.length);

        // 파일 전부 extract
        for (File file : dataFiles)
        {
            // 처리할 파일명 로그 print
        	Log.print("INFO : File NAME -> {0}", file.getAbsoluteFile());

        	try {
        		
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

                    if (eFile.getName().endsWith(".png"))
                    {    
                    	pngFileList.add(eFile);
                    	
                        if (DataFileStore.storeDateFile(eFile, processorInfo.FileSavePath))
                        {
                            
                        }
                    }
                }
                
                if (DataFileStore.storeDateFile(file, processorInfo.FileSavePath))
                {
                    query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, file.getName()));
                    queriesList.add(query);
                }

        		
        	} catch (Exception e) {
        		e.printStackTrace();
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
        
        for (int i=0 ; i<pngFileList.size() ; i++)
        {	
            if (pngFileList.get(i).exists())
            {
            	pngFileList.get(i).delete();
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