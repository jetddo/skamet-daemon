package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DataFileStore;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.model.image.LdpsWintemRegridImageGenerator;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * Created by chlee on 2017-02-15.
 */
public class UM_LOA_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "um_loa";
    private static final int DB_COLUMN_COUNT = 2;
    private static final int FILE_DATE_INDEX_POS = 2; // 20170101/qwumloa_pb000.nc.tgz
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // COL
    private final int INSERT_QUERY_1 = 1;
    private final int INSERT_QUERY_2 = 2;
    private final int DELETE_QUERY = 3;

    public UM_LOA_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }
 
    private void sendFileToAcom(List<Map<String, Object>> fileInfoList, ProcessorInfo processorInfo) {
    	
    	Log.print("INFO : Start Send Wintem Image Files");
    	
    	String host = "172.26.56.11";
    	String user = "kama";
    	String pwd = "kama1357!";
    	
    	try {
    		
    		FTPClient ftp = new FTPClient();
    		
    		ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));    		    		
    		ftp.connect(host);
    		int reply = ftp.getReplyCode();
    		
    		if(!FTPReply.isPositiveCompletion(reply)) {
    			ftp.disconnect();
    			throw new Exception("Exception in connecting to FTP Server");
    		}
    		
    		ftp.login(user, pwd);
    		ftp.setFileType(FTP.BINARY_FILE_TYPE);
    		ftp.enterLocalPassiveMode();
  
    		Calendar cal = new GregorianCalendar();
    		cal.setTime(processorInfo.FileDateFromNameOriginal);
    		
    		String issuedDtStr = new SimpleDateFormat("yyyyMMddHHmm").format(cal.getTime());
    		
        	for(int i=0 ; i<fileInfoList.size() ; i++) {
        		
        		Map<String, Object> fileInfo = fileInfoList.get(i);
        		
        		File imgFile = (File)fileInfo.get("imageFile"); 
        		File xmlFile = (File)fileInfo.get("xmlFile");
        		
        		String feet = (String)fileInfo.get("feet");
        		
        		String wintemImgFileName = imgFile.getName();
        		
        		Integer height = Integer.valueOf(wintemImgFileName.split("\\.")[0].split("_")[3]);
        		Integer hour = Integer.valueOf(wintemImgFileName.split("\\.")[0].split("_")[1].replace("pb", ""));
        		
        		String ftpImgFileName = "LOW_WINTEM_FL" + feet + "_" + String.format("%02d", hour) + "H_" + issuedDtStr + ".jpg";
        		String ftpXmlFileName = "LOW_WINTEM_FL" + feet + "_" + String.format("%02d", hour) + "H_" + issuedDtStr + ".xml";
        		        		
        		try(InputStream input = new FileInputStream(imgFile)) {
        			ftp.storeFile("/RCVD/KAMA/" + ftpImgFileName, input);
        		}
        		
        		try(InputStream input = new FileInputStream(xmlFile)) {
        			ftp.storeFile("/RCVD/KAMA/" + ftpXmlFileName, input);
        		}
        	}
        	
        	ftp.logout();
        	ftp.disconnect();
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    	Log.print("INFO : End Send Wintem Image Files");
    }
    
    private void generateLdpsWintemImageFiles(File ldpsFile, DatabaseManager dbManager, ProcessorInfo processorInfo) {
        
    	String ldpsImgSavePath = processorInfo.FileSavePath.replaceAll("/UM_LOA/", "/UM_LOA_WINTEM/");
     			
        File saveFolder = new File(ldpsImgSavePath);
      
        if (!saveFolder.exists() || saveFolder.isFile())
        {
            saveFolder.mkdirs();
        }
        
        LdpsWintemRegridImageGenerator ldpsWintemImageGenerator = new LdpsWintemRegridImageGenerator(dbManager, processorInfo);
        
        try {
        	
        	NetcdfDataset ncFile = NetcdfDataset.acquireDataset(ldpsFile.getAbsolutePath(), null);
            
        	List<Map<String, Object>> fileInfoList = ldpsWintemImageGenerator.generateImages(ncFile, ldpsFile.getName(), ldpsImgSavePath);
        	
//        	this.sendFileToAcom(fileInfoList, processorInfo);
        
            ncFile.close();	
            
        } catch (Exception e) {
        	e.printStackTrace();
        }
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
        	
        	try {
        		
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
                    	
                    	this.generateLdpsWintemImageFiles(eFile, dbManager, processorInfo);	
                   
                        if (DataFileStore.storeDateFile(eFile, processorInfo.FileSavePath))
                        {
                            query = buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, eFile.getName()));
                            queriesList.add(query);
                        }
                    }
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
        defineQueryFormat(INSERT_QUERY_1, "INSERT INTO %%WORKSPACE%%.NMDL_UM_LOA(FILE_DT, FILE_NM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(INSERT_QUERY_2, "INSERT INTO %%WORKSPACE%%.NMDL_UM_LOA_B(FILE_DT, FILE_NM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_UM_LOA");
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
