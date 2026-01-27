package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

import com.google.gson.Gson;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.image.GktgImageGenerator;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * Created by chlee on 2017-02-15.
 */
public class GKTG_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "gktg";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/visibility.nc
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;

    public GKTG_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }
    
    private void generateGktgImageFiles(File gktgFile, DatabaseManager dbManager, ProcessorInfo processorInfo, String attrName, String varName) {
        
    	String gktgImgSavePath = processorInfo.FileSavePath.replaceAll("/GKTG/", "/GKTG_IMG/");
    			
        File saveFolder = new File(gktgImgSavePath);
      
        if (!saveFolder.exists() || saveFolder.isFile())
        {
            saveFolder.mkdirs();
        }
        
        GktgImageGenerator gktgImageGenerator = new GktgImageGenerator(dbManager, processorInfo, attrName, varName);
        
        try {
        	
        	NetcdfDataset ncFile = NetcdfDataset.acquireDataset(gktgFile.getAbsolutePath(), null);
            
            List<BoundLonLat> jsonList = gktgImageGenerator.generateImages(ncFile, gktgFile.getName(), gktgImgSavePath); 

            ncFile.close();
            
            File file = new File(gktgImgSavePath + File.separator + gktgFile.getName() + ".json");

            FileWriter fw = new FileWriter(file);
            fw.write(new Gson().toJson(jsonList));
            fw.close();
            
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
            
            if(file.getName().contains("_max_")) {
            	this.generateGktgImageFiles(file, dbManager, processorInfo, "gtgmax", "GTGMAX");	
            } else if(file.getName().contains("_cat_")) {
            	this.generateGktgImageFiles(file, dbManager, processorInfo, "gtgdef", "GTGDEF");
            } else if(file.getName().contains("_mwt_")) {
            	this.generateGktgImageFiles(file, dbManager, processorInfo, "gtgmwt", "GTGMWT");
            }
                 
            if (DataFileStore.storeDateFile(file, processorInfo.FileSavePath))
            {
                query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, file.getName()));
                queriesList.add(query);
            }
        }
//
        for (File file : dataFiles)
        {
            if (file.exists())
            {
                file.delete();
            }
        }

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
