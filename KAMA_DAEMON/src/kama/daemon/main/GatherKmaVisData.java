package kama.daemon.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

/**
 * @author jetddo
 * Created on 2017-01-03
 */
public class GatherKmaVisData {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	//private final String GATHER_KMA_VIS_API_URL = "http://apigw.comis5.kma.go.kr/uapi/cgi-bin/url/nph-aws3_min_raw";
	
	private final String GATHER_KMA_VIS_API_URL = "http://api.kma.go.kr/url/kma_vis.php";
		
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
			
			System.out.println("Error : GatherKmaVisData.initialize -> " + e);
			
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
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmm");
		
		if(!this.initialize()) {
			
			System.out.println("Error : GatherKmaVisData.process -> initialize failed");
			return;
		}
		
		try {
			
			Date currentTm = new Date();
			
			// 자료가 1시간단위로 있는거 같다
			Calendar cal = new GregorianCalendar();
			cal.setTime(currentTm);
			cal.set(Calendar.MINUTE, 0);
			
			List<String[]> visRawDataList = this.gatherKmaVisData(cal.getTime());
			
			System.out.print("\t-> Gather [" + sdf2.format(cal.getTime()) + "] KMA VIS Data : ");
			
			System.out.print(visRawDataList.size() + " lines\n");
			
			System.out.println("\t-> Start Insert KMA VIS Datas ");
			
			for(int i=0 ; i<visRawDataList.size() ; i++) {
//				
//			//	System.out.println("\t\t-> Insert " + (i+1) + " line.. ");
//				
				String insertQuery = buildInsertQuery(visRawDataList.get(i));
//				
				this.dbManager.executeUpdate(insertQuery, false);
			}
			
			System.out.println("\t-> End Insert KMA VIS Datas ");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private String buildInsertQuery(String[] tokens) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
//		VISOBS0001 202506021300          100  11500  -99   10   1400     4677
		
		String insertQuery = " INSERT INTO AAMI.KMA_VIS_DATA VALUES (";
		
		for(int i=0 ; i<tokens.length ; i++) {
			
			String token = tokens[i];
			
			if(i==0) {
				insertQuery += "'" + token + "'";
			} else if(token.matches("[0-9]{12}") && i==1) {
				insertQuery += "TO_DATE('" + token + "','YYYYMMDDHH24MI')";
			} else {
				insertQuery += token;
			}
			
			if(i == tokens.length-1) {
				insertQuery += ")";
			} else {
				insertQuery += ",";
			}
		}
		
		return insertQuery;
	}
	
	private List<String[]> gatherKmaVisData(Date tm) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		String tmStr = sdf.format(tm);
		
		//http://api.kma.go.kr/url/kma_vis.php?tm=202506021350&type=&stn=&help=1

		String gatherURL = this.GATHER_KMA_VIS_API_URL + "?tm="+tmStr+"&type=&stn=&help=0";
		
		System.out.println("\t-> Gather URL: " + gatherURL);
		
		HttpURLConnection conn = (HttpURLConnection) new URL(gatherURL).openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
		conn.setReadTimeout(20000);
		conn.setRequestMethod("GET");
		
		OutputStream os = conn.getOutputStream();
		OutputStreamWriter writer = new OutputStreamWriter(os);
			
		writer.close();
		
		os.close();
		
		List<String[]> kmaVisRawDataList = new ArrayList<String[]>();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		String line = null;
		
		int cnt = 0;
		
		while((line = br.readLine()) != null) {
			
			if(++cnt <= 4 || line.contains("7777END")) {
				continue;
			}
			
			String[] tokens = line.split("\\s+");
			
			if(tokens.length == 8) {
				kmaVisRawDataList.add(tokens);
			}
		}
		
		br.close();
		conn.disconnect();
		
		return kmaVisRawDataList;
	}
	
	public static void main(String[] args) {

		new GatherKmaVisData().process();
	}
}