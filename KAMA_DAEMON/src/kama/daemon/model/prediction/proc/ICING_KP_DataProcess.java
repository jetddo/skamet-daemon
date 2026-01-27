package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.model.image.IcingKpImageGenerator;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * Created by chlee on 2017-02-15.
 */
public class ICING_KP_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "icing_kp";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/visibility.nc
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;
  
    public ICING_KP_DataProcess(DaemonSettings settings) throws IOException {
        super(settings, DATAFILE_PREFIX);
    }

    private void generateIcingEaImageFiles(File icingKpFile, DatabaseManager dbManager, ProcessorInfo processorInfo, String attrName, String varName) {
        
    	String icingKpImgSavePath = processorInfo.FileSavePath.replaceAll("/ICING_KP/", "/ICING_KP_IMG/");
    			
        File saveFolder = new File(icingKpImgSavePath);
      
        if (!saveFolder.exists() || saveFolder.isFile())
        {
            saveFolder.mkdirs();
        }
        
        IcingKpImageGenerator icingKpImageGenerator = new IcingKpImageGenerator(dbManager, processorInfo, attrName, varName);
        
        try {
        	
        	NetcdfDataset ncFile = NetcdfDataset.acquireDataset(icingKpFile.getAbsolutePath(), null);
            
        	icingKpImageGenerator.generateImages(ncFile, icingKpFile.getName(), icingKpImgSavePath); 

            ncFile.close();	
            
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Override@SuppressWarnings("Duplicates")
    protected void processDataByFileGroup(DatabaseManager dbManager, File[] dataFiles, ProcessorInfo processorInfo) throws Exception
    {
        String query = null;
        List<String> queriesList;

        queriesList = new ArrayList<>();

        // 파일 전부 extract
        for (File file : dataFiles)
        {
            // 처리할 파일명 로그 print
        	Log.print("INFO : File NAME -> {0}", file.getAbsoluteFile());
            
            this.generateIcingEaImageFiles(file, dbManager, processorInfo, "ICING_KP", "RAP");
                 
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
//
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
