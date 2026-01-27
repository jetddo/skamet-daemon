package kama.daemon.model.assessment.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.observation.adopt.WPU.WindProfiler;

public class RISK_MATRIX_GRID_DataProcess extends DataProcessor {
	
	// RISK MATRIX 데이터 파일명 Prefix
	private static final String DATAFILE_PREFIX = "risk_matrix_grid";
	private static final int DB_COLUMN_COUNT = 9;

	// RISK MATRIX 데이터 파일명을 '.' 문자로 분리하였을 때 날짜가 위치한 인덱스 번호
	private static final int DATAFILE_DATE_INDEX_POS = 6;	// Airport_feet_spd_sfc-upr_mem_00_20191010_00.txt

	private final int INSERT_QUERY = 1;
	
	private final int[][] SFC_HEIGHT = new int[][]{ {0, 250}, {0, 500}, {0, 1000}, {0, 1500}, {0, 2000}, {0, 2500}, {0, 3000}, {0, 4000} };
	private final int[][] UPR_HEIGHT = new int[][]{ {0, 250}, {250, 500}, {500, 1000}, {1000, 1500}, {1500, 2000}, {2000, 2500}, {2500, 3000}, {3000, 4000} };

	public RISK_MATRIX_GRID_DataProcess(final DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.AMDAR_KOR(TM, LATITUDE, LONGITUDE, ALTITUDE, FLIGHT_ID, TEMP,WD, WSPD, S_AIRPORT, D_AIRPORT, FLY_STAT) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'')");
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void processDataInternal(DatabaseManager dbManager, File file,
			ProcessorInfo processorInfo) throws Exception {
		// TODO Auto-generated method stub
		//
        // Your code begins here
        //        
        String fctDateFormatStr = "yyyyMMddHH";
        String tmDateFormatStr = "yyyy-MM-dd HH:mm:ss";

        SimpleDateFormat fctDateFormat = new SimpleDateFormat(fctDateFormatStr);
        SimpleDateFormat tmDateFormat = new SimpleDateFormat(tmDateFormatStr);
        // Airport_feet_spd_sfc-upr_mem_00_20191010_00.txt
        String[] fileNames = file.getName().split("[_|.]");
        System.out.println("filename:" + file.getName() + ",len:" + fileNames.length);
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
        	Date fctTm = null;
        	int member = -1;
        	int lowHeight = -1;
        	int highHeight = -1;
        	
        	String ele = null;
        	if(fileNames[2].equals("spd")) {
        		ele = "WIND_SPEED";
        	} else if(fileNames[2].equals("wdr")) {
        		ele = "WIND_DIRECTION";
        	}
        	
        	int[][] lev = (fileNames[3].equals("sfc-upr") ? SFC_HEIGHT : UPR_HEIGHT);
        	
            try
            {
            	fctTm = fctDateFormat.parse(fileNames[6] + fileNames[7]);
            	member = Integer.parseInt(fileNames[5]);
            }
            catch (ParseException pe)
            {
                throw new DaemonException(String.format("%s\n%s", "Error : unable to parse date from file name.", pe.toString()));
            }

            // Retrieve records using the previous source code
            List<Object[]> records = retrieveRecords(br, fctTm);
            System.out.println("len:" + records.size());
            
            int[] stnList = new int[]{113, 182, 110, 153, 163, 142, 167, 92, 151, 128, 158, 161, 139};
            
            Calendar cal = Calendar.getInstance();
            cal.setTime(fctTm);
            cal.add(Calendar.HOUR_OF_DAY, 4);
            
            for(int t = 0; t < 21; t++) {
            	for(int lv = 0; lv < lev.length; lv++) {
		            for(int i = 0; i < stnList.length; i++) {
		            	String query = "MERGE INTO AAMI.RISK_MATRIX " 
		            			+ " USING DUAL ON (STN_ID = %s AND MEMBER = %s AND FCT_TM = TO_DATE('%s', 'YYYY-MM-dd HH24:mi:ss') AND TM = TO_DATE('%s', 'YYYY-MM-dd HH24:mi:ss') AND LOW_HEIGHT = %s AND HIGH_HEIGHT = %s) "
		            			+ " WHEN MATCHED THEN UPDATE SET %s = %s "
		            			+ " WHEN NOT MATCHED THEN "
		            			+ " INSERT (STN_ID, MEMBER, FCT_TM, TM, LOW_HEIGHT, HIGH_HEIGHT, %s) "
		            			+ " VALUES (%s, %s, TO_DATE('%s', 'YYYY-MM-dd HH24:mi:ss'), TO_DATE('%s', 'YYYY-MM-dd HH24:mi:ss'), %s, %s, %s) ";
		            	String rst = String.format(query, 
		            			stnList[i], member, tmDateFormat.format(fctTm), tmDateFormat.format(cal.getTime()), lev[lv][0], lev[lv][1],
		            			ele, records.get(t+lv)[i],
		            			ele, 
		            			stnList[i], member, tmDateFormat.format(fctTm), tmDateFormat.format(cal.getTime()), lev[lv][0], lev[lv][1], records.get(t+lv)[i]);
		            	System.out.println(rst);
		            	dbManager.executeUpdate(rst);
		            }
            	}
            	cal.add(Calendar.HOUR_OF_DAY, 1);
            }
        }
	}
	
	private List<Object[]> retrieveRecords(BufferedReader br, Date fileTime) throws IOException, ArrayIndexOutOfBoundsException
    {
        List<Object[]> arrayOfRecords = new ArrayList<Object[]>();

        String line = null;
        while ((line = br.readLine()) != null)
        {
            // token : #
            String[] token = line.split(" ");
            arrayOfRecords.add(token);
        }

        return arrayOfRecords;
    }

}
