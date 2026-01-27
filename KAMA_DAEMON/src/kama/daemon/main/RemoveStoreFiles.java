package kama.daemon.main;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class RemoveStoreFiles {
	
	private final static String[] PATTERN_CONFIG = {"yyyy", "MM", "dd"};
	
	//private final static String REGEX_REMOVE_FILE = "(UKMO|LDAPS_ISOB|LDAPS_SGL|UM_LOA|UM_LOA_PC|UM_GLA|UM_REA|UM_VIS|RDAPS_ISOB|RDAPS_SGL|WAFC|RFOG|LFOG|KTG|GKTG|DFS|ICING_EA)";
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Configurations configs = new Configurations();
		DatabaseManager dbManager = null;

		try {
			
			Configuration config;

			config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			dbManager = DatabaseManager.getInstance();
			dbManager.setConfig(new DaemonSettings(config));
			dbManager.setAutoCommit(false);

			ResultSet rs = dbManager.executeQuery("SELECT FILE_CYCLE_DT FROM AAMI.SYS_FILE_CYCLE_OPT_I");
			
			Calendar c = new GregorianCalendar();	
			
			Date expiredDate = null;
			String dirRegex = null;
			
			if(rs.next()) {				
				
				c.add(Calendar.DATE, rs.getInt(1)*-1);
				
				expiredDate = c.getTime();
					
				System.out.println("expiredDate : "+(new SimpleDateFormat("yyyy-MM-dd")).format(expiredDate));				
			} 
			
			ResultSet rs2 = dbManager.executeQuery("SELECT DIR_REGEX FROM AAMI.SYS_FILE_CYCLE_LIST_OPT_I");
			
			if(rs2.next()) {
				dirRegex = rs2.getString(1);
				
				System.out.println("dirRegex : "+dirRegex);			
			}
			
			if(expiredDate != null && dirRegex != null) {
				
				final File baseDir = new File("/data/DATA_STORE");
				
				File[] dirs = baseDir.listFiles();
					
				for(File dir : dirs) {
					
					if(dir.getName().matches(dirRegex)) {
					
						recursiveProc(dir, expiredDate, 0);
					}
				}
			}
			
		} catch (ConfigurationException | SQLException e ) {
			
			System.out.println("Error : RemoveStoreFiles.recursiveProc -> " + e);
			
		} finally {
			
			dbManager.safeClose();			
		}
	}
		
	private static void recursiveProc(final File baseDir, final Date expiredDate, int depth) {
		
		File[] dirs = baseDir.listFiles();
		
		String pattern = PATTERN_CONFIG[depth];
		
		for(File dir : dirs) {
			
			if(dir.isDirectory()) {	
					
				Integer i = compareStringNumber(dir.getName(), (new SimpleDateFormat(pattern)).format(expiredDate));

				if(i == null) {
					continue;
				}
				
				if(i > 0) {
					
					System.out.println("Remove Directory -> " + dir.getAbsolutePath());
					
					try {
						FileUtils.deleteDirectory(dir);
					} catch(Exception e) {
						System.out.println("Error : RemoveStoreFiles.recursiveProc -> " + e);
					}
					
				} else if(i == 0) {
					
					if(depth < PATTERN_CONFIG.length-1) {
						recursiveProc(dir, expiredDate, depth+1);
					}
				}
			}
		}
	}
	
	private static Integer compareStringNumber(String s1, String s2) {
		
		try {
			
			return Integer.parseInt(s2)-Integer.parseInt(s1);
			
		} catch(NumberFormatException e) {
			System.out.println("Error : RemoveStoreFiles.recursiveProc -> " + e);
			return null;
		}
	}
}