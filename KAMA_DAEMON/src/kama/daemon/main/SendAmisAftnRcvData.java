package kama.daemon.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class SendAmisAftnRcvData {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private AmisDataBaseManager amisDbmanager;
	
	private final String getAmisAftnRecvDataList = 
			
			" SELECT TO_CHAR(TM, ''YYYYMMDDHH24MISS'') AS TM, "+ 
			"	MSG_TYPE, "+
			"	DOMAIN, "+
			"	MSG_ID, "+
			"	TO_CHAR(TM_IN, ''YYYYMMDDHH24MISS'') AS TM_IN, "+
			"	TO_CHAR(TR_TM, ''YYYYMMDDHH24MISS'') AS TR_TM, "+
			"	MSG_ORG, "+
			"	MSG_STS, "+
			"	MSG_DIR, "+
			"	MSG_ADDR, "+
			"	DOMAIN_LIST, "+
			"	MSG_SRC, "+
			"	SENDER, "+
			"	AFTN_SRC "+
			" FROM AMISUSER.AFTN_RCV "+
			" WHERE TM >= TO_DATE(''{0}'', ''YYYYMMDDHH24MI'') AND TM <= TO_DATE(''{1}'', ''YYYYMMDDHH24MI'') ORDER BY TM ASC ";
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.amisDbmanager = new AmisDataBaseManager(this.config);
			this.amisDbmanager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : SendAmisAftnRcvData.initialize -> " + e);
			
			this.amisDbmanager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.amisDbmanager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : SendAmisAftnRcvData.process -> initialize failed");
			return;
		}
		
		final String tempDir = "/data/temp/";
		
		final int dataInterval = 30;
		
		try {
			
			Calendar cal = new GregorianCalendar();
			
			Date currentTm = new Date();
			
			cal.setTime(currentTm);
			cal.add(Calendar.HOUR_OF_DAY, -9);
			cal.set(Calendar.SECOND, 0);
			cal.add(Calendar.MINUTE, -cal.get(Calendar.MINUTE) % dataInterval);
			
			String endTm = sdf.format(cal.getTime());
			
			cal.add(Calendar.MINUTE, -dataInterval);
			
			String startTm = sdf.format(cal.getTime());
			
			System.out.println("startTm: " + startTm);
			System.out.println("endTm: " + endTm);
			
			List<Map<String, Object>> aftnRcvDataList = this.amisDbmanager.select(MessageFormat.format(this.getAmisAftnRecvDataList, new Object[]{
				startTm, endTm	
			}));
			
			System.out.println("aftnRcvDataList length = " + aftnRcvDataList.size());
			
			final String fileName = "AFTN_RCV_AMO_" + endTm + "00";
			
			System.out.println("query: " +  MessageFormat.format(this.getAmisAftnRecvDataList, new Object[]{
				startTm, endTm	
			}));
			
			System.out.println("fileName: " + fileName);
			
			BufferedWriter fw = new BufferedWriter(new FileWriter(tempDir + fileName));
			
			fw.write("TM,MSG_TYPE,DOMAIN,MSG_ID,TM_IN,TR_TM,MSG_ORG,MSG_STS,MSG_DIR,MSG_ADDR,DOMAIN_LIST,MSG_SRC,SENDER,AFTN_SRC\n");
			
			for(int i=0 ; i<aftnRcvDataList.size() ; i++) {
				
				Map<String, Object> aftnRcvData = aftnRcvDataList.get(i);
				
				fw.write(aftnRcvData.get("tm") == null ? "" :  aftnRcvData.get("tm") + ",");
				fw.write(aftnRcvData.get("msgType") == null ? "" :  aftnRcvData.get("msgType") + ",");
				fw.write(aftnRcvData.get("domain") == null ? "" :  aftnRcvData.get("domain").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("msgId") == null ? "" :  aftnRcvData.get("msgId").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("tmIn") == null ? "" :  aftnRcvData.get("tmIn") + ",");
				fw.write(aftnRcvData.get("trTm") == null ? "" :  aftnRcvData.get("trTm") + ",");
				fw.write(aftnRcvData.get("msgOrg") == null ? "" :  aftnRcvData.get("msgOrg").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("msgSts") == null ? "" :  aftnRcvData.get("msgSts").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("msgDir") == null ? "" :  aftnRcvData.get("msgDir").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("msgAddr") == null ? "" :  aftnRcvData.get("msgAddr").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("domainList") == null ? "" :  aftnRcvData.get("domainList").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("msgSrc") == null ? "" :  aftnRcvData.get("msgSrc").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("sender") == null ? "" :  aftnRcvData.get("sender").toString().replaceAll("\\s+", " ") + ",");
				fw.write(aftnRcvData.get("aftnSrc") == null ? "" : aftnRcvData.get("aftnSrc").toString().replaceAll("\\s+", " ") + "\n");
			}
			
			fw.flush();
			
			fw.close();
			
			File file = new File(tempDir + fileName);
			
			if(!file.exists()) {
				System.out.println("error: file is not exist");
				return;
			}
			
			System.out.println("Start Send File");
	    	
	    	String host = "172.26.56.11";
	    	String user = "kama";
	    	String pwd = "kama1357!";	    	

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
  
    		try(InputStream input = new FileInputStream(file)) {
    			ftp.storeFile("/data3/amis/SNDD/COMIS_AFTN/" + file.getName(), input);
    		}
        	
        	ftp.logout();
        	ftp.disconnect();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		this.destroy();
	}
	
	public static void main(String[] args) {

		new SendAmisAftnRcvData().process();
	}
}