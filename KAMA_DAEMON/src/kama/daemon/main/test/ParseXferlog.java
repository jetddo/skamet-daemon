package kama.daemon.main.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;

import kama.daemon.common.db.DatabaseManager;

public class ParseXferlog {
	
	private DatabaseManager dbManager;
	
	private Configuration config;
	
	private boolean initialize() {
		
		
		
		return true;
	}
	
	private void destroy() {
		
	}

	private void parseXferlogFile(File file) {
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
			
			String line = null;
			
			Map<String, List<Map<String, String>>> fileDataMap = new HashMap<String, List<Map<String, String>>>();
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				String[] tokens = line.split("\\s+");
				
				String ip = tokens[6];
				String fullFileName = tokens[8];
				
				String[] fullFileNameTokens = fullFileName.split("/");
				
				String fileName = null;
				
				if(fullFileNameTokens[1].matches("[0-9]{10}")) {
					fileName = fullFileNameTokens[2];					
				} else {
					fileName = fullFileNameTokens[1];
				}
				
				int count = 0;
				
				for (int j = 0; j < fileName.length(); j++) {           
					
					if (fileName.charAt(j) == '_') {                
						count++;            
					}        
				}
				
				if(count == 0) {
					continue;
				}				 
				
				String[] fileNameTokens = fileName.split("_");
				
				String fileKey = fileNameTokens[0];
				
				if(count > 1) {
					
					if(!fileNameTokens[1].matches("[0-9]+")) {
						fileKey = fileNameTokens[0] + "_" + fileNameTokens[1];	
					} 
                }
				
				List<Map<String, String>> fileDataList = fileDataMap.get(fileKey);
				
				if (fileDataList == null) {
					fileDataList = new ArrayList<Map<String, String>>();
					fileDataMap.put(fileKey, fileDataList);
				}
				
				Map<String, String> fileData = new HashMap<String, String>();
				fileData.put("ip", ip);
				fileData.put("fileName", fileName);
				fileData.put("fullFileName", fullFileName);
				fileDataList.add(fileData);
			}
			
			br.close();
			
			for (String key : fileDataMap.keySet()) {
				
				
				
                List<Map<String, String>> fileDataList = fileDataMap.get(key);
                
                if(fileDataList.size() > 0) {
                	
                	//System.out.println(fileDataList.size());                	
                	System.out.println(fileDataList.get(0).get("ip") + "\t" + fileDataList.get(0).get("fileName") + "\t" + (fileDataList.size()));
                	//System.out.println("---------------------------------------------------");
                }
            }
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) throws Exception {
		
		File file = new File("F:/data/xferlog-20241110");
		
		ParseXferlog test = new ParseXferlog();
		test.initialize();		
		test.parseXferlogFile(file);
		test.destroy();
	}

}
