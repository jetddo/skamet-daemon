package kama.daemon.model.observation.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.observation.adopt.AMDAR_DataConv;
import kama.daemon.model.observation.adopt.IEODO.IEODOData;

public class IEODO_DataProcess extends DataProcessor {

	private static final String DATAFILE_PREFIX = "ieodo";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
	
	public IEODO_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}
	
	protected List<IEODOData> parseFile(File file) throws IOException, ParseException {
		String line;
		String[] token;
		
		// Declare your column variables
		List<IEODOData> lstIEODO;
		
		lstIEODO = new ArrayList<IEODOData>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			// HEADER 파싱
			while((line = br.readLine()) != null) {
				Pattern infoPattern = Pattern.compile("^[0-9]{4}(/)[0-9]{1,2}(/)[0-9]{1,2}");
				Matcher infoMatcher = infoPattern.matcher(line);
				if(!infoMatcher.find()){
					continue;
				}
				// token : ' '
				token = line.split(",");
				IEODOData ieodo = new IEODOData();

				// Null 데이터표시 : '-'
				// 관측시간,풍속 10분,최대풍속,풍향 10분,최대풍향,온도,습도,기압,일사,일조,강우량,가시 거리,조위,유의파고,주기,유의파고2,최대파고,운량,운고,CTR 전도도,CTR 수온,RDCP유속,RDCP유향,오존
				
				// 관측시각
				ieodo.tm = DateFormatter.parseDate(String.format("%s", token[0]), "yyyy/MM/dd HH:mm:ss");	// 2019/10/20 23:54:00
				
				ieodo.ws = (("-").equals(token[1]) ? Float.NaN : Float.parseFloat(token[1])); // 풍속 10분
				ieodo.ws_gst = (("-").equals(token[2]) ? Float.NaN : Float.parseFloat(token[2])); // 최대풍속
				ieodo.wd = (("-").equals(token[3]) ? Float.NaN : Float.parseFloat(token[3])); // 풍향 10분
				ieodo.wd_gst = (("-").equals(token[4]) ? Float.NaN : Float.parseFloat(token[4])); // 최대풍향
				ieodo.ta = (("-").equals(token[5]) ? Float.NaN : Float.parseFloat(token[5])); // 온도
				ieodo.hm = (("-").equals(token[6]) ? Float.NaN : Float.parseFloat(token[6])); // 습도
				ieodo.pa = (("-").equals(token[7]) ? Float.NaN : Float.parseFloat(token[7])); // 기압
				ieodo.si = (("-").equals(token[8]) ? Float.NaN : Float.parseFloat(token[8])); // 일사
				ieodo.ss = (("-").equals(token[9]) ? Float.NaN : Float.parseFloat(token[9])); // 일조
				ieodo.rn_int = (("-").equals(token[10]) ? Float.NaN : Float.parseFloat(token[10])); // 강우량
				ieodo.vi = (("-").equals(token[11]) ? Float.NaN : Float.parseFloat(token[11])); // 가시 거리
				ieodo.ls = (("-").equals(token[12]) ? Float.NaN : Float.parseFloat(token[12])); // 조위
				ieodo.wh_sig = (("-").equals(token[13]) ? Float.NaN : Float.parseFloat(token[13])); // 유의파고
				ieodo.wp = (("-").equals(token[14]) ? Float.NaN : Float.parseFloat(token[14])); // 주기
				ieodo.wh_sig2 = (("-").equals(token[15]) ? Float.NaN : Float.parseFloat(token[15])); // 유의파고2
				ieodo.wh_max = (("-").equals(token[16]) ? Float.NaN : Float.parseFloat(token[16])); // 최대파고
				ieodo.cla_1lyr = (("-").equals(token[17]) ? Float.NaN : Float.parseFloat(token[17])); // 운량
				ieodo.base_1lyr = (("-").equals(token[18]) ? Float.NaN : Float.parseFloat(token[18])); // 운고
				ieodo.sa = (("-").equals(token[19]) ? Float.NaN : Float.parseFloat(token[19])); // CTR 전도도(??) -> 염분 필드에 삽입
				ieodo.tw = (("-").equals(token[20]) ? Float.NaN : Float.parseFloat(token[20])); // CTR 수온
				ieodo.sc = (("-").equals(token[21]) ? Float.NaN : Float.parseFloat(token[21])); // RDCP유속
				ieodo.sd = (("-").equals(token[22]) ? Float.NaN : Float.parseFloat(token[22])); // RDCP유향
				ieodo.o3 = (("-").equals(token[23]) ? Float.NaN : Float.parseFloat(token[23])); // 오존
				
				lstIEODO.add(ieodo);
			}
		}
		
		return lstIEODO;
		
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		// 관측시간,풍속 10분,최대풍속,풍향 10분,최대풍향,온도,습도,기압,일사,일조,강우량,가시 거리,조위,유의파고,주기,유의파고2,최대파고,운량,운고,CTR 전도도,CTR 수온,RDCP유속,RDCP유향,오존
 		defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.EXT_KORDI_IEODO(TM, WS, WS_GST, WD, WD_GST, TA, HM, PA, SI, SS, RN_INT, VI, LS, WH_SIG, WP, WH_SIG2, WH_MAX, CLA_1LYR, BASE_1LYR, SA, TW, SC, SD, O3) " 
		+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'', ''{20}'', ''{21}'', ''{22}'', ''{23}'')");
	}

	/**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        List<IEODOData> lstIEODO;
        String query;
        
        lstIEODO = parseFile(file);
        
        for(IEODOData ieodo : lstIEODO) {
        	String[] token = convertToRecordFormat(ieodo);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) token);
            dbManager.executeUpdate(query, false);
        }

    }
	
	private String[] convertToRecordFormat(IEODOData ieodo) {
		List<String> lstTokens;
		String[] tokens;
		
		lstTokens = new ArrayList<String>();

		lstTokens.add(convertToDBText(ieodo.tm));
		lstTokens.add(convertToDBText(ieodo.ws));
		lstTokens.add(convertToDBText(ieodo.ws_gst));
		lstTokens.add(convertToDBText(ieodo.wd));
		lstTokens.add(convertToDBText(ieodo.wd_gst));
		lstTokens.add(convertToDBText(ieodo.ta));
		lstTokens.add(convertToDBText(ieodo.hm));
		lstTokens.add(convertToDBText(ieodo.pa));
		lstTokens.add(convertToDBText(ieodo.si));
		lstTokens.add(convertToDBText(ieodo.ss));
		lstTokens.add(convertToDBText(ieodo.rn_int));
		lstTokens.add(convertToDBText(ieodo.vi));
		lstTokens.add(convertToDBText(ieodo.ls));
		lstTokens.add(convertToDBText(ieodo.wh_sig));
		lstTokens.add(convertToDBText(ieodo.wp));
		lstTokens.add(convertToDBText(ieodo.wh_sig2));
		lstTokens.add(convertToDBText(ieodo.wh_max));
		lstTokens.add(convertToDBText(ieodo.cla_1lyr));
		lstTokens.add(convertToDBText(ieodo.base_1lyr));
		lstTokens.add(convertToDBText(ieodo.sa));
		lstTokens.add(convertToDBText(ieodo.tw));
		lstTokens.add(convertToDBText(ieodo.sc));
		lstTokens.add(convertToDBText(ieodo.sd));
		lstTokens.add(convertToDBText(ieodo.o3));
		
		tokens = new String[lstTokens.size()];
		
		return lstTokens.toArray(tokens);
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return FILE_DATE_INDEX_POS;
	}

}
