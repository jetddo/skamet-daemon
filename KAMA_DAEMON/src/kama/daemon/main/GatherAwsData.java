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
public class GatherAwsData {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	//private final String GATHER_AWS_API_URL = "http://apigw.comis5.kma.go.kr/uapi/cgi-bin/url/nph-aws3_min_raw";
	
	private final String GATHER_AWS_API_URL = "http://api.kma.go.kr/cgi-bin/url/nph-aws3_min_raw";
		
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
			
			System.out.println("Error : GatherAwsData.initialize -> " + e);
			
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
			
			System.out.println("Error : GatherAwsData.process -> initialize failed");
			return;
		}
		
		try {
			
			Date currentTm = new Date();
			
			int[] minDiffs = {-1, 0, 1};
			
			for(int minDiff: minDiffs) {
				
				Calendar cal = new GregorianCalendar();
				cal.setTime(currentTm);
				
				cal.add(Calendar.MINUTE, minDiff);
				
				List<String[]> awsRawDataList = this.gatherAwsData(cal.getTime());
				
				System.out.print("\t-> Gather [" + sdf2.format(cal.getTime()) + "] AWS3 Data : ");
				
				System.out.print(awsRawDataList.size() + " lines\n");
				
				System.out.println("\t-> Start Insert AWS3 Datas ");
				
				for(int i=0 ; i<awsRawDataList.size() ; i++) {
					
				//	System.out.println("\t\t-> Insert " + (i+1) + " line.. ");
					
					String deleteQuery = buildDeleteQuery(awsRawDataList.get(i));
					
					this.dbManager.executeQuery(deleteQuery);
					
					String insertQuery = buildInsertQuery(awsRawDataList.get(i));
					
					this.dbManager.executeUpdate(insertQuery, false);
				}
				
				System.out.println("\t-> End Insert AWS3 Datas ");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private String buildDeleteQuery(String[] tokens) {
		
		String deleteQuery = " DELETE AAMI.KMA_NPH_AWS3_MIN WHERE TM = TO_DATE('"+tokens[0]+"', 'YYYYMMDDHH24MI') AND STN_ID = "+tokens[1];
		
		return deleteQuery;
	}
	
	private String buildInsertQuery(String[] tokens) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		String insertQuery = " INSERT INTO AAMI.KMA_NPH_AWS3_MIN VALUES (TO_DATE('"+tokens[0]+"', 'YYYYMMDDHH24MI'),";
		
		for(int i=1 ; i<tokens.length ; i++) {
			
			String token = tokens[i];
			
			insertQuery += token;
			
			if(i == tokens.length-1) {
				insertQuery += ")";
			} else {
				insertQuery += ",";
			}
		}
		
		return insertQuery;
	}
	
	private List<String[]> gatherAwsData(Date tm) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		String tmStr1 = sdf.format(tm);
		String tmStr2 = sdf.format(tm);
		
		//HttpURLConnection conn = (HttpURLConnection) new URL(this.GATHER_AWS_API_URL + "?tm1="+tmStr1+"&tm2="+tmStr2+"&gov=KMA&stn_sp=SS:SA&stn=&disp=0&help=1&authKey=eyJraWQiOiJHQ0tXU05aQlJ5Q2lsa2pXUWZjZ3ZBIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJhdWQiOiJBUElQVEwiLCJiZHM6c2lkIjoiNjY3YTQ3NjI1OTQ4MzE2NDUyMzc1Nzc4NmQzMjQyMzk1ODU5NjUzMTU5NDEiLCJiZHM6ZW5jcmQiOiJwQUdrdTFnZmw3YW1rV3ctem1wU1dRIiwiaXNzIjoiaHR0cDovL3Nzb3B0bC5jb21pczUua21hLmdvLmtyIiwiYmRzOmNoa2hzIjoiVlB1eHVDR0lhc2tsbXBoRmVOT1U1NEFkNnZTa3FGN3RkRUh1TXpIeWstQ05laVUyRGhxaFVsRUlZa0RKdUxOdE5EemJVb0hFZ0phVHNVbmxPZHJPWEEiLCJleHAiOjI1NTYwMjUxNzgsImV4cGlyZXNfaW4iOjg0OTE3MjU2MCwiaWF0IjoxNzA2ODUyNjE4LCJqdGkiOiJWc1NYU29pdVJGdUVsMHFJcnJSYnJnIiwiY2xpZW50X2lkIjoiQVBJUFRMIiwiYmRzOnRva2VuX3Jlc190eXBlIjoidG9rZW4ifQ.NZT8xa-HsKSr5L8Qkcu-JZokE7JD2kgbX7jrC4sFPuyygoyW8eMuy8-qnfGcCF-J8KzDEhWeU27Q1chswEP-zNij1IWehVCBdQ8vtd751g-V26qHnq1hZHD6CyobT3VPG5kEdVkgbP4HZivAAAmjAN1wwsqm8NX9N8s5aYpBY_x2-e1P_K5pRY2IOcojb8X6qzSFN3rX-pNC0so9cSTSLWJC0s9lGoUGAHPoSJvZjLVoSZGMJqAvRB_T9MsQ0-6Bfp7Bi-gAYB3b4S-bEdtpwota-ib1lbZwbA6-3jfEibpHdXE1jxpY-QOhips6XqANjNPmMqb0-czHf4mSFgiwDA" ).openConnection();

		String gatherURL = this.GATHER_AWS_API_URL + "?tm1="+tmStr1+"&tm2="+tmStr2+"&gov=KMA&stn_sp=SS:SA&stn=&disp=0&help=1";
		
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
		
		List<String[]> awsRawDataList = new ArrayList<String[]>();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		String line = null;
		
		int cnt = 0;
		
		while((line = br.readLine()) != null) {
			
			if(cnt++ > 1) {
				
				try {
				
					String[] tokens = line.split(",");
					
					List<String> list = new ArrayList<String>();
					
					for(int i=0 ; i<=89 ; i++) {
						list.add(tokens[i].trim());
					}
					
					awsRawDataList.add(list.toArray(new String[list.size()]));
					
				} catch (Exception e) {
					
				}
			}
		}
		
		br.close();
		conn.disconnect();
		
		return awsRawDataList;
	}
	
	public static void main(String[] args) {

		new GatherAwsData().process();
	}
}