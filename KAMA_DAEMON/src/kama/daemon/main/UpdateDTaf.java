package kama.daemon.main;

import java.io.File;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.AMF;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class UpdateDTaf {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	
	private final String getUmLoaPcMaxDtQuery = 
			
			" SELECT DISTINCT TO_CHAR(MAX(FILE_DT), 'YYYYMMDDHH24') AS FILE_DT FROM AAMI_TEST.DMON_FILE_PROC_H WHERE FILE_CD = 'UM_LOA_PC' ORDER BY FILE_DT DESC ";
		
	private final String insertDtaf = 
			
			" INSERT INTO AAMI.DTAF(STN_CD, ISSUED_DT, DTAF) VALUES " + 
			" (''{0}'', TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'') "; 
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : UpdateDtaf.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");		
		
		if(!this.initialize()) {
			
			System.out.println("Error : UpdateDtaf.process -> initialize failed");
			return;
		}
		
		Date ldapsPcMaxIssuedDt = null;
		
		try {
			
			ResultSet resultSet = this.dbManager.executeQuery(this.getUmLoaPcMaxDtQuery);
			resultSet.next();
			ldapsPcMaxIssuedDt = sdf.parse(resultSet.getString(1));
			
		} catch (Exception e) {
			
		}
		
		Object[][] airportInfoList = new Object[][]{
			
				{"RKSI",126.431224,37.461891},
				{"RKNY",128.663433,38.058909},
				{"RKJY",127.614272,34.840029},
				{"RKSS",126.795059,37.558582},
				{"RKPC",126.491235,33.510342},
				{"RKJB",126.387598,34.993573},
				{"RKPU",129.355368,35.593075},
				{"RKPK",128.946202,35.173273},
				{"RKTU",127.495685,36.721994},
				{"RKTN",128.638623,35.899504},
				{"RKJJ",126.810784,35.139808},
				{"RKTH",129.433654,35.984659},
				{"RKPS",128.086236,35.092310}					
		};
		
		String coordinatesLatPath = this.config.getString("um_loa.coordinates.lat.path");
		String coordinatesLonPath = this.config.getString("um_loa.coordinates.lon.path");
		String storePath = this.config.getString("global.storePath.unix");
		
		for(int i=0 ; i<airportInfoList.length ; i++) {
			
			String stnCd = airportInfoList[i][0].toString();
			
			float lat = Float.valueOf(airportInfoList[i][2].toString());
			float lon = Float.valueOf(airportInfoList[i][1].toString());
			
			String dtaf = AMF.createDigitalTaf(storePath, coordinatesLatPath, coordinatesLonPath, ldapsPcMaxIssuedDt, lat, lon);
			
			if(dtaf != null) {
				
				this.dbManager.executeUpdate(MessageFormat.format(this.insertDtaf, new Object[]{
					stnCd, sdf.format(ldapsPcMaxIssuedDt), dtaf
				}));	
			}
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	public static void main(String[] args) {

		new UpdateDTaf().process();
	}
}