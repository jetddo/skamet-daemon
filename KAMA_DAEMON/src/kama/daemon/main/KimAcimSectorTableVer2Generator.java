package kama.daemon.main;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointLonLat;
import kama.daemon.common.util.model.PointXY;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimAcimSectorTableVer2Generator {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String ACIM_PREFIX = "amo_gdps_acim_cnvt_f";
	
	private final String insertFileProcInfo = 
			
			" INSERT INTO AAMI.STORED_FILE_PROC_H(FILE_DT, FILE_NAME, FILE_PATH, PROC_DT, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), '{fileName}', '{filePath}', SYSDATE, 'KIM_ACIM_CNVT_VER2') "; 
	
	private final String selectFileProcInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.STORED_FILE_PROC_H							"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'KIM_ACIM_CNVT_VER2'					";
	
	String[][][] areaInfo = null;
	String[] areaThresholds = null;
	String[] areaCoords = null;
	Color[] areaBackgroundColors = null;
	
	String[][][] areaInfo1 = {

		{ { "서해" } },
		{ { "동해" } }, 
		{ { "포항" } }, 
		{ { "대구" } }, 
		{ { "남해" } }, 
		{ { "군산" } }, 
		{ { "광주" } }, 
		{ { "제주북부" } },
		{ { "제주남부" } },
		{ { "제주남부" } }, 
		{ { "서울TMA" } }, 
		{ { "서울TMA(이·착륙)" } },
		{ { "서울TMA(이·착륙)" } }, 
		{ { "김해TMA" } }, 
		{ { "김해TMA(이·착륙)" } },		
		{ { "제주APP" } }
	};
	
	String[][][] areaInfo2 = {

		{ { "서해 Y697·Y644" } },
		{ { "동해 Y697·Y437" } }, 
		{ { "포항 Y685" } }, 
		{ { "대구 Y782·Z83" } }, 
		{ { "남해 Y571·Y572" } }, 
		{ { "군산 Y711·Y722" } }, 
		{ { "광주 Y711·Y722" } }, 
		{ { "RUGMA→GUKDO" } }
	};
	
	// 각 구역별 대류운 면적 산정에 사용할 고도 범위(ft)
	// 99999는 상한 없음, 0 시작은 해당 고도 이하를 의미함
	String[] areaThresholds1 = new String[] { 
		"18000~99999", 
		"18000~99999", 
		"26000~99999", 
		"26000~99999", 
		"18000~99999", 
		"15000~99999", 
		"21000~99999", 
		"29000~99999", 
		"24000~99999", 
		"29000~99999", 
		"0~10000", 
		"0~5000", 
		"0~10000", 
		"0~16000", 
		"0~16000", 
		"0~16000"
	};
	
	String[] areaThresholds2 = new String[] { 
		"8000~41000", 
		"8000~41000", 
		"8000~41000", 
		"8000~41000", 
		"8000~41000",
		"14000~41000",
		"8000~41000",
		"8000~41000"
	};
	
	Color[] areaBackgroundColors1 = new Color[] {

		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),

		new Color(226,239,218),
		new Color(226,239,218),
		new Color(226,239,218),
		new Color(226,239,218),
		new Color(226,239,218),

		new Color(255,217,102),
		new Color(255,217,102),
		new Color(255,217,102),
		new Color(255,217,102),
		new Color(255,217,102),
		new Color(255,217,102)
	}; 
	
	Color[] areaBackgroundColors2 = new Color[] {

		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),
		new Color(221,235,247),

		new Color(226,239,218),
		new Color(226,239,218),
		new Color(226,239,218)
	}; 
	
	String[] areaCoords1 = new String[] {

		// 1. 서해
		"38.0000|124.0000,38.0000|124.8500,38.3389|127.6644,37.0361|127.6644,36.9711|127.5611,37.1194|127.2311,37.0839|126.9375,36.9472|125.8061,36.9086|125.6000,36.3333|125.6000,36.3333|124.0000,38.0000|124.0000",
		
		// 2. 동해
		"38.3389|127.6644,38.6333|128.3667,38.6333|129.8475,37.1194|129.8475,36.3364|129.8478,37.1194|128.6811,37.1194|127.8489,37.1111|127.6644,38.3389|127.6644",
		
		// 3. 포항
		"36.9711|127.5611,37.0361|127.6644,37.1111|127.8489,37.1194|128.6811,36.3364|129.8478,36.1750|131.1675,35.8239|130.7236,35.4117|130.1700,35.4117|129.1569,35.9036|129.0811,36.9711|127.5611",
		
		// 4. 대구
		"36.7386|127.1911,36.8383|127.3486,36.9711|127.5611,35.9036|129.0811,35.4117|129.1569,35.4117|130.1700,35.2089|129.8847,34.7886|129.3231,34.7231|129.2333,34.6667|129.1667,34.5031|129.0178,34.5031|128.4989,34.7653|128.4989,35.2103|128.4989,35.5031|127.8311,35.5178|127.8186,35.7531|127.6144,35.8636|127.6144,36.2031|127.6144,36.7386|127.1911",
		
		// 5. 남해
		"35.5031|127.8311,35.2103|128.4989,34.5031|128.4989,34.5031|129.0178,33.7540|128.4517,33.6256|127.3314,33.9144|127.3314,34.2533|127.3314,35.1520|127.3144,35.2197|127.6478,35.5031|127.8311",
		
		// 6. 군산
		"36.3333|124.0000,36.3333|125.6000,36.9086|125.6000,36.9472|125.8061,37.0839|126.9375,37.1194|127.2311,36.9711|127.5611,36.8383|127.3486,36.7386|127.1911,36.2031|127.6144,35.7531|127.6144,35.5031|127.8311,35.5031|126.6397,35.5031|124.0000,36.3333|124.0000",
		
		// 7. 광주
		"35.5031|124.0000,35.5031|127.8311,35.2197|127.6478,35.1520|127.3144,34.2533|127.3314,34.0833|127.3314,33.8814|126.7325,33.8417|126.5672,33.8417|124.0000,35.5031|124.0000",
		
		// 8. 제주북부
		"33.8417|124.0000,33.8417|126.5672,33.8814|126.7325,34.0833|127.3314,33.6256|127.3314,33.7540|128.4517,32.5000|127.5000,32.5000|126.8333,32.5397|126.2600,32.4799|125.9831,32.0414|124.0000,33.8417|124.0000",
		
		// 9. 제주남부
		"32.0414|124.0000,32.4799|125.9831,32.5397|126.2600,32.5000|126.8333,30.0000|125.4167,30.0000|125.1983,30.0000|124.9533,30.0000|124.0000,31.6100|124.0000,32.0414|124.0000",
		
		// 10. 제주남부
		"32.0414|124.0000,32.4799|125.9831,32.5397|126.2600,32.5000|126.8333,30.0000|125.4167,30.0000|125.1983,30.0000|124.9533,30.0000|124.0000,31.6100|124.0000,32.0414|124.0000",
		
		// 11. 서울TMA
		"37.3525|125.5506,37.0419|125.6992,36.7664|126.4869,36.7222|126.9986,37.0047|127.6317,37.9525|127.6536,37.6386|126.8861,37.7131|126.6733,37.6436|126.1592,37.3525|125.5506",
		
		// 12. 서울TMA(이·착륙)
		"37.5703|126.0100,36.9639|126.4836,37.1406|127.4094,37.6944|126.5803,37.5703|126.0100",
		
		// 13. 서울TMA(이·착륙)
		"37.5703|126.0100,36.9639|126.4836,37.1406|127.4094,37.6944|126.5803,37.5703|126.0100",
		
		// 14. 김해TMA
		"35.5031|128.4978,35.4797|128.5611,35.5033|128.7739,35.5031|129.0311,35.4197|129.0894,35.4197|129.8311,35.2364|129.7978,34.5031|128.8644,34.5031|128.4978,35.5031|128.4978",

		// 15. 김해TMA(이·착륙)
		"35.4803|128.5772,35.4894|128.9469,35.2183|129.0728,35.2597|129.7994,35.1906|129.7903,34.5106|128.9383,34.5161|128.4978,35.4803|128.5772",

		// 16. 제주APP
		"33.0033|125.8314,34.2531|125.8314,34.2531|125.9981,34.2533|127.3314,33.2700|127.3314,33.0033|126.9981,33.0033|125.9981,33.0033|125.8314"
	};
	
	String[] areaCoords2 = new String[] {

		// 1. 서해 Y697·Y644
		"37.2564|124.0000,37.3075|124.0878,37.4492|124.3661,37.4686|124.8347,37.4778|125.2328,37.4839|125.6494,37.4969|126.5097,37.5031|126.9267,37.5628|127.3294,37.6133|127.6389,37.4594|127.6875,37.4225|127.4339,37.3764|127.1833,37.1956|127.5036,37.0875|127.6958,36.9531|127.5786,37.0683|127.3744,37.1356|127.1225,37.0539|126.9394,37.1297|126.8442,37.1269|126.4708,37.1142|125.8003,37.1178|125.4872,37.1108|124.8333,36.9772|124.5211,36.6803|124.5189,36.6794|124.3117,37.0856|123.9992,37.2564|124.0000",

		// 2. 동해 Y697·Y437
		"37.7589|128.5844,37.7825|128.7719,37.7694|129.7025,37.7664|129.8656,37.7575|130.2875,37.7206|131.4983,37.7236|132.1033,37.7244|132.7767,37.7492|133.0072,37.5831|132.9928,37.5592|132.7767,37.3681|132.8086,37.2592|132.6622,37.5600|132.5694,37.5633|131.6408,36.8011|130.8114,36.4361|131.4967,36.3106|131.3600,36.7414|130.5247,37.1078|129.7792,37.3792|129.2236,37.6175|128.7539,37.5922|128.5844,37.5236|128.1353,37.4503|127.6628,37.6139|127.6528,37.6903|128.1353,37.7589|128.5844",

		// 3. 포항 Y685
		"37.0711|127.7208,36.9831|127.8775,36.8950|128.0339,36.7750|128.2456,36.6833|128.4064,36.3886|128.9286,36.0378|129.5356,36.0114|129.8669,35.8956|130.7803,35.7808|130.6253,35.8603|129.7797,35.9167|129.4133,36.2858|128.7664,36.5806|128.2442,36.6722|128.0833,36.7922|127.8717,36.8803|127.7153,36.9683|127.5586,37.0711|127.7208",

		// 4. 대구 Y782·Z83
		"36.8772|127.4394,36.7533|127.5917,36.4922|127.9100,36.3181|128.1208,35.8333|128.6861,35.5178|128.8753,35.1536|129.0947,34.8450|129.3919,34.6739|129.1642,34.6739|129.1642,34.5508|129.0694,34.7961|128.8783,34.5269|128.4947,34.5792|128.4236,34.7117|128.4217,35.1269|128.3956,35.4842|128.4617,35.7831|128.4914,36.2403|127.9392,36.4144|127.7283,36.6756|127.4100,36.7994|127.2578,36.8772|127.4394",

		// 5. 남해 Y752·Y511
		"34.5700|128.5633,34.1925|128.0419,33.6972|127.3306,34.0444|127.3358,34.3181|127.6664,34.8719|128.4308,34.5700|128.5633",
		
		// 6. 군산 Y711·Y722
		"37.1097|127.1533,36.9014|127.4431,36.8383|127.3486,36.7369|127.0944,36.5728|127.1008,36.3839|127.1069,35.9756|127.0592,35.9639|127.2678,35.9467|127.6206,35.7806|127.6083,35.7978|127.2556,35.8056|127.0414,35.5031|127.0081,35.5031|126.2956,36.7469|126.5133,37.0839|126.9375,37.1097|127.1533",
			
		// 7. 광주 Y711·Y722
		"35.5075|126.2828,35.5031|127.0081,35.3581|126.9914,35.5931|127.7744,35.4425|127.8628,35.2128|127.5897,35.2122|127.3144,35.0456|127.3144,35.0486|126.9564,33.9033|126.8389,33.8808|126.7325,33.8417|126.5672,33.8417|126.0050,35.5075|126.2828",

		// 8. RUGMA→GUKDO
		"35.5031|127.3108,36.4144|127.4472,36.2031|127.6144,35.5031|127.5153,35.1836|127.4675,34.2706|127.3311,34.0733|127.3017,34.0003|127.0861,35.5031|127.3108"
		
	};
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private String storePath = null;


	/**
	 * 테이블 레이아웃 설정
	 */
	private static class TableLayoutConfig {

		private final int imageWidth;
		private final int imageHeight;

		private final int tableLeftMargin;
		private final int tableTopMargin;
		private final int tableRightMargin;

		private final int rowHeight;
		private final int labelColWidth;
		private final int remarkColWidth;
		
		private String dataTypeKor;

		private TableLayoutConfig(int imageWidth, int imageHeight,
				int tableLeftMargin, int tableTopMargin, int tableRightMargin,
				int rowHeight, int labelColWidth, int remarkColWidth, String dataTypeKor) {

			this.imageWidth = imageWidth;
			this.imageHeight = imageHeight;
			this.tableLeftMargin = tableLeftMargin;
			this.tableTopMargin = tableTopMargin;
			this.tableRightMargin = tableRightMargin;
			this.rowHeight = rowHeight;
			this.labelColWidth = labelColWidth;
			this.remarkColWidth = remarkColWidth;
			this.dataTypeKor = dataTypeKor;
		}
	}

	/**
	 * 테이블 폰트 설정
	 */
	private static class TableFontConfig {

	    private final int dayHeaderFontSize;     // 날짜(MM-dd)
	    private final int timeHeaderFontSize;    // 00,03,06...
	    private final int bodyFontSize;          // 데이터
	    private final int legendFontSize;        // 범례

	    private TableFontConfig(
	            int dayHeaderFontSize,
	            int timeHeaderFontSize,
	            int bodyFontSize,
	            int legendFontSize) {

	        this.dayHeaderFontSize = dayHeaderFontSize;
	        this.timeHeaderFontSize = timeHeaderFontSize;
	        this.bodyFontSize = bodyFontSize;
	        this.legendFontSize = legendFontSize;
	    }
	}

	/**
	 * dataType별 테이블 레이아웃 설정을 반환합니다.
	 */
	private TableLayoutConfig getTableLayoutConfig(int dataType) {

		switch (dataType) {

		case 1:
			return new TableLayoutConfig(
					1300, 840,
					40, 170, 40,
					30, 190, 120, "섹터"
			);

		case 2:
			return new TableLayoutConfig(
					1300, 840,
					40, 170, 40,
					44, 190, 120, "항로"
			);

		default:
			throw new IllegalArgumentException("Unsupported dataType: " + dataType);
		}
	}

	/**
	 * dataType별 테이블 폰트 설정을 반환합니다.
	 * 행 높이와 독립적으로 폰트 크기를 조정할 수 있습니다.
	 */
	private TableFontConfig getTableFontConfig(int dataType) {

		switch (dataType) {

		case 1:
		    return new TableFontConfig(
		            15,   // 날짜
		            15,   // 시간
		            13,   // 본문
		            15    // 범례
		    );

		case 2:
		    return new TableFontConfig(
		            15,   // 날짜(고정)
		            15,   // 시간(고정)
		            15,   // 본문만 증가
		            15
		    );

		default:
			throw new IllegalArgumentException("Unsupported dataType: " + dataType);
		}
	}
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			storePath = this.config.getString("global.storePath.unix");
			
//			storePath = "\\\\172.26.56.124\\data_store";
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
	
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : KimAcimSectorTableGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private List<Map<String, Object>> getSectorDataList(int dataType) {

		List<Map<String, Object>> sectorDataList = new ArrayList<Map<String, Object>>();

		int idx = 0;

		for (int a = 0; a < areaInfo.length; a++) {
			for (int b = 0; b < areaInfo[a].length; b++) {

				if (idx >= areaCoords.length || idx >= areaThresholds.length || idx >= areaBackgroundColors.length) {
					System.out.println("Warning : Area static data count mismatch. index = " + idx);
					break;
				}

				Map<String, Object> sectorData = new HashMap<String, Object>();

				// 왼쪽 표시는 단일 라벨을 사용하며, 동일한 라벨이 연속되면 rowspan 처리
				sectorData.put("labelName", areaInfo[a][b][0]);
				sectorData.put("areaCode", String.format("%02d", idx + 1));
				sectorData.put("areaThreshold", areaThresholds[idx]);
				sectorData.put("areaBackgroundColor", areaBackgroundColors[idx]);

				List<double[]> areaPolygon = new ArrayList<double[]>();
				String[] coords = areaCoords[idx].split(",");

				for (String coord : coords) {

					String[] latLon = coord.split("\\|");

					if (latLon.length == 2) {
						double lat = Double.parseDouble(latLon[0]);
						double lon = Double.parseDouble(latLon[1]);

						areaPolygon.add(new double[] { lon, lat });
					}
				}

				sectorData.put("areaPolygon", areaPolygon);

				sectorDataList.add(sectorData);

				idx++;
			}
		}

		return sectorDataList;
	}
	
	/**
	 * KIM ACIM 모델 파일을 반환합니다.
	 * 
	 * @return NetcdfDataset 배열
	 */
	private Map<String, List<NetcdfDataset>> getAcimModelFileMap() {
		
		Map<String, List<NetcdfDataset>> acimModelFileMap = new HashMap<String, List<NetcdfDataset>>();
		
		// DB 처리 이력과 datastore 파일 목록을 비교하여
		// 아직 처리되지 않은 KIM ACIM 예측 파일 세트를 조회한다.
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // datastore 디렉터리 경로 생성용		
		
		System.out.println("Store Path : " + storePath);
		
		Calendar cal = new GregorianCalendar();
		
		try {			
			//"2026031800"
			cal.setTime(new Date());
			cal.add(Calendar.HOUR_OF_DAY, -9-12);
			cal.add(Calendar.HOUR_OF_DAY, -cal.get(Calendar.HOUR_OF_DAY)%6); // 6시간 단위로 맞추기 위해 현재 시간을 6시간 단위로 내림 처리
			// 현재 시각 기준으로 최근 예측 세트부터 확인
			
			String targetDtStr = sdf.format(cal.getTime());
			
			String query = this.selectFileProcInfoList.replaceAll("\\{targetDt\\}", targetDtStr);			
			
			List<Map<String, Object>> parsedFileInfoList = new ArrayList<Map<String, Object>>();
			
			ResultSet resultSet = dbManager.executeQuery(query);
			
			while(resultSet.next()) {
				
				Map<String, Object> parsedFileInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);				
				parsedFileInfoList.add(parsedFileInfo);
			}
			
			Map<String, List<String>> parsedFileInfoMap = new HashMap<String, List<String>>();
			
			for (Map<String, Object> parsedFileInfo : parsedFileInfoList) {
				String fileDt = parsedFileInfo.get("fileDt").toString();
				String fileName = parsedFileInfo.get("fileName").toString();

				if (!parsedFileInfoMap.containsKey(fileDt)) {
					parsedFileInfoMap.put(fileDt, new ArrayList<String>());
				}

				parsedFileInfoMap.get(fileDt).add(fileName);
			}
			
			Map<String, Object> parsedIssuedTmMap = new HashMap<String, Object>();
			
			for (Map.Entry<String, List<String>> entry : parsedFileInfoMap.entrySet()) {
				
				String fileDt = entry.getKey();
				List<String> fileNames = entry.getValue();

				// f00부터 f39까지 3시간 간격 파일이 모두 처리되었는지 확인
				boolean allFilesPresent = true;
				
				for (int i = 0; i < 40; i+=3) {
					
					String expectedFileName = this.ACIM_PREFIX + String.format("%02d", i) + "_" + fileDt + ".nc";
					
					if (!fileNames.contains(expectedFileName)) {
						allFilesPresent = false;
						break;
					}
				}

				if (allFilesPresent) {
					parsedIssuedTmMap.put(fileDt, new Object());		
				}
			}
			
			for(int i=0 ; i<3 ; i++) {
				
				Date targetDt = cal.getTime();			
				
				String targetDirStr = storePath + File.separator + "/KIM_ACIM_CNVT/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.print("Target Dir : " + targetDir.getAbsolutePath());
				
				// 이미 처리내역에 있으면 건너뛴다
				if(parsedIssuedTmMap.containsKey(sdf2.format(targetDt))) {
					System.out.println(" -> already processed");
				} else 
				{
					
					if(targetDir.exists()) {
						
						File[] files = targetDir.listFiles(new FilenameFilter() {

							@Override
							public boolean accept(File f, String name) {
								
								return name.startsWith(ACIM_PREFIX) && name.endsWith(".nc");
							}							
						});
						
						// 대상 디렉터리에 f00~f39까지 3시간 간격 파일이 모두 있는지 확인
						
						boolean allFilesPresent = true;
						
						for (int j = 0; j < 40; j+=3) {
							
							String expectedFileName = this.ACIM_PREFIX + String.format("%02d", j) + "_" + sdf2.format(targetDt) + ".nc";
								
							boolean fileFound = false;
							for (File file : files) {
								if (file.getName().equals(expectedFileName)) {
									fileFound = true;
									break;
								}
							}
							if (!fileFound) {
								allFilesPresent = false;
								break;
							}
						}
						
						List<NetcdfDataset> modelFileList = new ArrayList<NetcdfDataset>();
						
						if (allFilesPresent) {
							
							System.out.println(" -> checked");
							
							// 모든 예측시간 파일이 존재하는 경우에만 NetCDF 파일 목록 생성
							for (File file : files) {
								
								NetcdfDataset ncFile = NetcdfDataset.acquireDataset(file.getAbsolutePath(), null);
								modelFileList.add(ncFile);
							}
							
							Collections.sort(modelFileList, new Comparator<NetcdfDataset>() {

								@Override
								public int compare(NetcdfDataset f1, NetcdfDataset f2) {
									
									try {
					                	
					                	String fcstHour1 = f1.getLocation().substring(f1.getLocation().lastIndexOf("f") + 1, f1.getLocation().lastIndexOf("_"));
					                	String fcstHour2 = f2.getLocation().substring(f2.getLocation().lastIndexOf("f") + 1, f2.getLocation().lastIndexOf("_"));
					                	
										int hour1 = Integer.parseInt(fcstHour1);
										int hour2 = Integer.parseInt(fcstHour2);
										return Integer.compare(hour1, hour2); // 예측시간 기준 오름차순 정렬 
					                   
					                } catch (Exception e) {
					                    e.printStackTrace();
					                    return 0;
					                }
								}
								
							});
							
							// 발행시각별 모델 파일 목록 저장
							acimModelFileMap.put(sdf2.format(targetDt), modelFileList);
						} else {
							System.out.println(" -> skipped (not all files present)");
						}
					} else {
						System.out.println(" -> skipped (target directory does not exist)");
					}
				}
				
				cal.add(Calendar.HOUR_OF_DAY, 6);	
			}
			
			return acimModelFileMap;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return acimModelFileMap;
	}
	
	private void destroyModelFileList(List<NetcdfDataset> modelFileList) {

		if (modelFileList != null && !modelFileList.isEmpty()) {
			for (NetcdfDataset ncFile : modelFileList) {
				try {
					ncFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void bindSectorTableStaticData(int dataType) {
		
		switch(dataType) {
		
		case 1:
			
			areaInfo = areaInfo1;
			areaCoords = areaCoords1;
			areaThresholds = areaThresholds1;
			areaBackgroundColors = areaBackgroundColors1;
			
			break;
			
			
		case 2:

			areaInfo = areaInfo2;
			areaCoords = areaCoords2;
			areaThresholds = areaThresholds2;
			areaBackgroundColors = areaBackgroundColors2;			
			
			break;
		
		}
	}
	
	private boolean generateSectorTable(String issuedTmStr, List<NetcdfDataset> modelFileList, int dataType) {

		bindSectorTableStaticData(dataType);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("MM-dd");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH");

		try {

			String latPath = config.getString("kim_gktg.coordinates.lat.path");
			String lonPath = config.getString("kim_gktg.coordinates.lon.path");

			ModelGridUtil modelGridUtil = new ModelGridUtil(
					ModelGridUtil.Model.KIM_GKTG,
					ModelGridUtil.Position.MIDDLE_CENTER,
					latPath,
					lonPath
			);

			modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(50, 20, 110, 150);

			List<Map<String, Object>> sectorDataList = this.getSectorDataList(dataType);

			int fcstHourLength = 39;
			int fcstHourInterval = 3;
			int fcstHourSize = fcstHourLength / fcstHourInterval + 1;

			if (modelFileList == null || modelFileList.size() < fcstHourSize) {

				System.out.println(
						"Error : Model file count is insufficient. required = "
						+ fcstHourSize
						+ ", actual = "
						+ (modelFileList == null ? 0 : modelFileList.size())
				);

				return false;
			}

			TableLayoutConfig layout = this.getTableLayoutConfig(dataType);
			TableFontConfig fontConfig = this.getTableFontConfig(dataType);

			String dataTypeKor = layout.dataTypeKor;
			
			int width = layout.imageWidth;
			int height = layout.imageHeight;

			int tableLeftMargin = layout.tableLeftMargin;
			int tableTopMargin = layout.tableTopMargin;
			int tableRightMargin = layout.tableRightMargin;
			int fixedRowHeight = layout.rowHeight;

			int tableWidth = width - tableLeftMargin - tableRightMargin;

			int labelColIndex = 0;
			int leftHeaderCols = 1;

			int totalRows = sectorDataList.size() + 2;

			int dayHeaderRowHeight = 35;
			int timeHeaderRowHeight = 35;
			int dataRowHeight = layout.rowHeight;

			int tableHeight =
			        dayHeaderRowHeight
			        + timeHeaderRowHeight
			        + dataRowHeight * sectorDataList.size();

			/*
			 * 범례 위치 계산에 사용하기 위한 하단 여백.
			 * 표 높이가 달라지면 자동으로 변경된다.
			 */
			int tableBottomMargin = height - tableTopMargin - tableHeight;

			int labelColWidth = layout.labelColWidth;
			int remarkColWidth = layout.remarkColWidth;

			int remarkColIndex = leftHeaderCols + fcstHourSize;
			int totalCols = remarkColIndex + 1;

			int[] colWidths = new int[totalCols];

			colWidths[labelColIndex] = labelColWidth;
			colWidths[remarkColIndex] = remarkColWidth;

			int fixedColWidth = labelColWidth + remarkColWidth;
			int timeColWidth = (tableWidth - fixedColWidth) / fcstHourSize;
			int timeExtraWidth = (tableWidth - fixedColWidth) % fcstHourSize;

			for (int c = 0; c < fcstHourSize; c++) {

				int timeColIndex = leftHeaderCols + c;

				colWidths[timeColIndex] = timeColWidth;

				if (c < timeExtraWidth) {
					colWidths[timeColIndex]++;
				}
			}

			int[] colXList = new int[totalCols];
			colXList[0] = tableLeftMargin;

			for (int c = 1; c < totalCols; c++) {
				colXList[c] = colXList[c - 1] + colWidths[c - 1];
			}

			int[] rowYList = new int[totalRows];
			int[] rowHeightList = new int[totalRows];

			int currentY = tableTopMargin;

			for (int r = 0; r < totalRows; r++) {

			    rowYList[r] = currentY;

			    if (r == 0) {
			        rowHeightList[r] = dayHeaderRowHeight;
			    } else if (r == 1) {
			        rowHeightList[r] = timeHeaderRowHeight;
			    } else {
			        rowHeightList[r] = dataRowHeight;
			    }

			    currentY += rowHeightList[r];
			}

			/*
			 * 폰트 크기는 행 높이와 분리하여 dataType별 설정을 사용한다.
			 */
			Font dayHeaderFont = this.getFont(fontConfig.dayHeaderFontSize, true);
			Font timeHeaderFont = this.getFont(fontConfig.timeHeaderFontSize, true);
			Font bodyFont = this.getFont(fontConfig.bodyFontSize, true);
			Font legendFont = this.getFont(fontConfig.legendFontSize, true);

			BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
			Graphics2D g = image.createGraphics();

			g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
			g.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);

			g.setColor(Color.WHITE);
			g.fillRect(0, 0, width, height);

			Date issuedTm = sdf.parse(issuedTmStr);
			Calendar cal = new GregorianCalendar();

			this.createFrameInfo(g, width, height, issuedTm, dataType);

			System.out.println("\n::: Start Generate Sector Table :::");
			System.out.println("-> Issued Time: " + sdf3.format(issuedTm) + ", Model File Count: " + modelFileList.size());
			System.out.println("-> Data Type: " + dataType + ", Label Column Count: 1");

			int currentDay = 0;
			boolean dayChanged = false;

			for (int j = 0; j < totalCols; j++) {

				int x = colXList[j];
				int currentCellWidth = colWidths[j];

				boolean isLabelCol = j == labelColIndex;
				boolean isRemarkCol = j == remarkColIndex;
				boolean isTimeCol = j >= leftHeaderCols && j < remarkColIndex;

				Float[][] koreaData = null;

				if (isTimeCol) {

					int fcstIndex = j - leftHeaderCols;

					cal.setTime(issuedTm);
					cal.add(Calendar.HOUR_OF_DAY, fcstHourInterval * fcstIndex);

					int newDay = cal.get(Calendar.DAY_OF_MONTH);

					if (currentDay == 0) {
						currentDay = newDay;
					} else if (currentDay != newDay) {
						dayChanged = true;
						currentDay = newDay;
					}

					NetcdfDataset ncFile = modelFileList.get(fcstIndex);
					Variable var = ncFile.findVariable("CCT");

					if (var == null) {

						System.out.println("Error : CCT variable not found. " + ncFile.getLocation());

						g.dispose();

						return false;
					}

					koreaData = this.getAcimModelKoreaData(ncFile, modelGridUtil, var);
				}

				for (int i = 0; i < totalRows; i++) {

					int y = rowYList[i];
					int currentCellHeight = rowHeightList[i];

					boolean isDayHeaderRow = i == 0;
					boolean isColumnHeaderRow = i == 1;
					boolean isDataRow = i > 1;

					Map<String, Object> sectorData = null;

					if (isDataRow) {
						sectorData = sectorDataList.get(i - 2);
					}

					// rowspan 대상인 왼쪽 라벨 컬럼은 각 행마다 배경을 다시 칠하지 않는다.
					if (!isDataRow || !isLabelCol) {
						g.setColor(Color.WHITE);
						g.fillRect(x, y, currentCellWidth, currentCellHeight);
					}

					if (isColumnHeaderRow) {

						g.setColor(Color.BLACK);

						if (isTimeCol) {

							String fcstHourText = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY));

							this.setCellText(
									fcstHourText,
									g,
									x,
									y,
									currentCellWidth,
									currentCellHeight,
									timeHeaderFont
							);
						}

					} else if (isDataRow) {

						if (i == 2) {

							g.setColor(Color.BLACK);
							g.setStroke(new BasicStroke(1));
							g.drawLine(tableLeftMargin, y - 3, tableLeftMargin + tableWidth, y - 3);
						}

						if (isLabelCol) {

							int dataRowIndex = i - 2;
							String key = "labelName";

							boolean drawCell = this.isRowSpanStart(sectorDataList, dataRowIndex, key);

							if (drawCell) {

								int spanCount = this.getRowSpanCount(sectorDataList, dataRowIndex, key);
								int spanHeight = 0;

								for (int k = 0; k < spanCount; k++) {
									spanHeight += rowHeightList[i + k];
								}

								Color bg = (Color) sectorData.get("areaBackgroundColor");

								g.setColor(bg);
								g.fillRect(x, y, currentCellWidth, spanHeight);

								g.setColor(Color.BLACK);

								this.setCellText(
										sectorData.get(key).toString(),
										g,
										x,
										y,
										currentCellWidth,
										spanHeight,
										bodyFont
								);

								g.setStroke(new BasicStroke(1));
								g.drawRect(x, y, currentCellWidth, spanHeight);
							}

						} else if (isRemarkCol) {

							g.setColor(Color.BLACK);

							String remarkText = this.getRemarkText(sectorData.get("areaThreshold").toString());

							this.setCellText(
									remarkText,
									g,
									x,
									y,
									currentCellWidth,
									currentCellHeight,
									bodyFont
							);

						} else if (isTimeCol) {

							float thresholdRatio = this.getAcimModelAvgDataInSector(
									sectorData,
									modelGridUtil,
									koreaData
							);

							Color thresholdColor = null;

							if (thresholdRatio <= 10) {
								thresholdColor = Color.WHITE;
							} else if (thresholdRatio < 20) {
								thresholdColor = new Color(128, 228, 16);
							} else if (thresholdRatio < 30) {
								thresholdColor = new Color(0, 183, 80);
							} else if (thresholdRatio < 40) {
								thresholdColor = new Color(255, 252, 0);
							} else if (thresholdRatio < 50) {
								thresholdColor = new Color(255, 130, 0);
							} else {
								thresholdColor = new Color(255, 0, 0);
							}

							g.setColor(thresholdColor);
							g.fillRect(x, y, currentCellWidth, currentCellHeight);
						}
					}

					if (isDayHeaderRow && isTimeCol && j >= leftHeaderCols + 1) {

						if (dayChanged || j == remarkColIndex - 1) {

							int h = 0;
							Date d = cal.getTime();

							if (dayChanged) {

								g.setColor(Color.BLACK);
								g.setStroke(new BasicStroke(1));
								g.drawRect(x, y, currentCellWidth, currentCellHeight);

								dayChanged = false;

								h = Math.min(
										24,
										(int) ((d.getTime() - issuedTm.getTime()) / 1000 / 60 / 60)
								);

								d = new Date(d.getTime() - 1000 * 60);

							} else {
								h = cal.get(Calendar.HOUR_OF_DAY) - 3;
							}

							g.setColor(Color.BLACK);

							this.setCellText(
									sdf2.format(d),
									g,
									x - h * currentCellWidth / fcstHourInterval / 2,
									y,
									0,
									currentCellHeight,
									dayHeaderFont
							);
						}

					} else {

						// 왼쪽 데이터 라벨은 rowspan 셀에서 직접 테두리를 그린다.
						if (!(isDataRow && isLabelCol)) {

							g.setColor(Color.BLACK);
							g.setStroke(new BasicStroke(1));
							g.drawRect(x, y, currentCellWidth, currentCellHeight);
						}
					}
				}
			}

			int remarkX = colXList[remarkColIndex];
			int remarkY = rowYList[0];
			int remarkW = colWidths[remarkColIndex];
			int remarkH = rowHeightList[0] + rowHeightList[1];

			g.setColor(Color.WHITE);
			g.fillRect(remarkX, remarkY, remarkW, remarkH);

			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(1));
			g.drawRect(remarkX, remarkY, remarkW, remarkH);

			this.setCellText("고도", g, remarkX, remarkY, remarkW, remarkH, timeHeaderFont);

			int mergedX = tableLeftMargin;
			int mergedY = rowYList[0];
			int mergedW = colWidths[labelColIndex];
			int mergedH = rowHeightList[0] + rowHeightList[1];

			g.setColor(Color.WHITE);
			g.fillRect(mergedX, mergedY, mergedW, mergedH);

			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(1));
			g.drawRect(mergedX, mergedY, mergedW, mergedH);
			g.drawLine(mergedX, mergedY, mergedX + mergedW, mergedY + mergedH);

			g.setColor(Color.BLACK);
			g.setFont(timeHeaderFont);

			FontMetrics fm = g.getFontMetrics();

			String rightTopText = "TIME(UTC)";

			int leftTextX = mergedX + 18;
			int leftTextY = mergedY + mergedH - 12;

			int rightTextX = mergedX + mergedW - fm.stringWidth(rightTopText) - 18;
			int rightTextY = mergedY + fm.getAscent() + 8;

			g.drawString(dataTypeKor, leftTextX, leftTextY);
			g.drawString(rightTopText, rightTextX, rightTextY);

			g.setStroke(new BasicStroke(2));
			g.setColor(Color.BLACK);
			g.drawRect(tableLeftMargin, tableTopMargin, tableWidth, tableHeight);

			this.createLegend(g, width, height, tableRightMargin, legendFont, dataType);

			g.dispose();

			String y = issuedTmStr.substring(0, 4);
			String m = issuedTmStr.substring(4, 6);
			String d = issuedTmStr.substring(6, 8);

			String imgFileDirPath = storePath + "/CDM_RPT/CLD/" + y + "/" + m + "/" + d;
			//String imgFileDirPath = "C:/data/datastore/CDM_RPT/CLD/" + y + "/" + m + "/" + d;
			File imgFileDir = new File(imgFileDirPath);

			if (!imgFileDir.exists()) {
				imgFileDir.mkdirs();
			}

			String suffix = dataType == 1 ? "sector_" : "route_";

			File imgFile = new File(
					imgFileDirPath
					+ "/cld_fct_"
					+ suffix
					+ issuedTmStr
					+ ".png"
			);

			ImageIO.write(image, "png", imgFile);

			System.out.println("-> Create KIM ACIM Sector Table Image: " + imgFile.getAbsolutePath());

		} catch (Exception e) {

			e.printStackTrace();

			return false;
		}

		return true;
	}
	
	private String getRemarkText(String threshold) {

		String[] arr = threshold.split("~");

		int min = Integer.parseInt(arr[0]);
		int max = Integer.parseInt(arr[1]);

		if (min == 0) {
			return "FL" + String.format("%03d", max / 100) + " 이하";
		}

		if (max == 99999) {
			return "FL" + String.format("%03d", min / 100) + " 이상";
		}

		return "FL" + String.format("%03d", min / 100) + "~FL" + String.format("%03d", max / 100);
	}
	
	private void createFrameInfo(Graphics2D g, int width, int height, Date issuedTm, int dataType) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy년 MM월 dd일 HHmm");
		
		// 문서 전체 외곽 프레임을 그린다
		
		int leftMargin = 20;
		int topMargin = 20;
		int rightMargin = 20;
		int bottomMargin = 20;		
        
        int logoSize = 125;
        int titleHeight = 65;
        
        try {
        	
            String logoFilePath = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res") + File.separator + "amo_logo.png";
            
            BufferedImage logoImg = ImageIO.read(new File(logoFilePath));
            
            g.drawImage(logoImg, leftMargin+10, topMargin+10, logoSize-20, logoSize-20, null);

        	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		g.setColor(Color.BLACK);
		g.setStroke(new BasicStroke(2)); // 외곽선
        g.drawRect(leftMargin, topMargin, width - leftMargin - rightMargin, height - topMargin - bottomMargin);
        
        g.setStroke(new BasicStroke(1)); // 내부 구분선
        g.drawLine(leftMargin, topMargin + logoSize, width - rightMargin, topMargin + logoSize); // 로고/제목 영역 하단 구분선
        
        // 왼쪽 상단 로고 영역 테두리
        
        g.drawRect(leftMargin, topMargin, logoSize, logoSize);
        
        g.drawLine(leftMargin + logoSize, topMargin + titleHeight, width - rightMargin, topMargin + titleHeight); // 제목/발표정보 구분선
		
        Font titleFont = this.getFont(30, true);
        Font infoFont = this.getFont(20, true);
        g.setFont(titleFont);
        g.setColor(Color.BLACK);
        
        String titleText = "";
        
        if(dataType == 1) {
        	titleText = "대류운 예측 정보 (섹터)";
        } else if(dataType == 2) {
        	titleText = "대류운 예측 정보 (항로)";
        } 
        
        g.drawString(titleText, this.getCellTextLeftMargin(titleText, g, width)+50, topMargin + titleFont.getSize() + 10); // 제목 표시
        
        g.setFont(infoFont);
        g.drawString("발표시각: " + sdf.format(issuedTm) + "UTC", leftMargin + logoSize + 15, topMargin + titleHeight + infoFont.getSize() + 15); // 발표시각 표시
        
        String agency = "항공기상청 예보과";
        g.drawString(agency, width - agency.length()*infoFont.getSize(), topMargin + titleHeight + infoFont.getSize() + 15); // 기관명 표시
		
	}
	
	private void createLegend(Graphics2D g, int width, int height, int tableRightMargin, Font font, int dataType) {

	    String[] thresholds = { "10~19", "20~29", "30~39", "40~49", "50~" };

	    Color[] colors = {
	        new Color(128, 228, 16),
	        new Color(0, 183, 80),
	        new Color(255, 252, 0),
	        new Color(255, 130, 0),
	        new Color(255, 0, 0)
	    };

	    int legendWidth = width / 3;
	    int legendHeight = 25;

	    int legendLeftMargin = width - legendWidth - tableRightMargin;

	    // 이미지 하단 기준으로 고정
	    int legendBottomMargin = 75;	    

	    if(dataType == 2) {
	    	legendBottomMargin = 195;
	    }
	    
	    int legendTopMargin = height - legendBottomMargin - legendHeight;
	
	    int cellWidth = legendWidth / thresholds.length;
	    int cellExtraWidth = legendWidth % thresholds.length;
	    int cellHeight = legendHeight;

	    g.setColor(Color.WHITE);
	    g.fillRect(
	            legendLeftMargin,
	            legendTopMargin,
	            legendWidth,
	            legendHeight
	    );

	    g.setFont(font);
	    g.setColor(Color.BLACK);

	    String legendTitle = "대류운 면적(%): ";

	    g.drawString(
	            legendTitle,
	            legendLeftMargin - legendTitle.length() * font.getSize() / 3 * 2,
	            legendTopMargin + font.getSize()
	    );

	    int currentX = legendLeftMargin;

	    for (int i = 0; i < thresholds.length; i++) {

	        int currentCellWidth = cellWidth;

	        if (i < cellExtraWidth) {
	            currentCellWidth++;
	        }

	        g.setColor(colors[i]);
	        g.fillRect(
	                currentX,
	                legendTopMargin,
	                currentCellWidth,
	                cellHeight
	        );

	        g.setColor(Color.BLACK);

	        this.setCellText(
	                thresholds[i],
	                g,
	                currentX - 2,
	                legendTopMargin - 1,
	                currentCellWidth,
	                cellHeight,
	                font
	        );

	        g.setStroke(new BasicStroke(1));
	        g.drawRect(
	                currentX,
	                legendTopMargin,
	                currentCellWidth,
	                cellHeight
	        );

	        currentX += currentCellWidth;
	    }

	    g.setColor(Color.BLACK);
	    g.setStroke(new BasicStroke(2));

	    g.drawRect(
	            legendLeftMargin,
	            legendTopMargin,
	            legendWidth,
	            legendHeight
	    );

	    g.drawString(
	            "기반모델: KIM GDAPS",
	            legendLeftMargin - legendTitle.length() * font.getSize() / 3 * 2,
	            legendTopMargin + (int) (font.getSize() * 3.5)
	    );
	}
	
	private int getCellTextLeftMargin(String text, Graphics2D g, int cellWidth) {

		FontMetrics fm = g.getFontMetrics();
		int textWidth = fm.stringWidth(text);

		return (cellWidth - textWidth) / 2;
	}
	
	private void setCellText(String text, Graphics2D g, int x, int y,
			int cellWidth, int cellHeight, Font font) {

		g.setFont(font);

		FontMetrics fm = g.getFontMetrics();

		int drawX = x + (cellWidth - fm.stringWidth(text)) / 2;
		int drawY = y + (cellHeight - fm.getHeight()) / 2 + fm.getAscent();

		g.drawString(text, drawX, drawY);
	}
	
	private Float[][] getAcimModelKoreaData(NetcdfDataset ncFile, ModelGridUtil modelGridUtil, Variable var) {

		try {

			BoundXY boundXY = modelGridUtil.getBoundXY();
			
			int rows = modelGridUtil.getRows();
			int cols = modelGridUtil.getCols();
			
			List<Range> rangeList = new ArrayList<Range>();
			rangeList.add(new Range(boundXY.getBottom(), boundXY.getTop()));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);

			return values;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private float getAcimModelAvgDataInSector(Map<String, Object> sectorData, ModelGridUtil modelGridUtil, Float[][] koreaData) {
				
		String areaThreshold = (String) sectorData.get("areaThreshold"); // 구역별 산정 고도 범위
		
		int areaThresholdMin = Integer.valueOf(areaThreshold.split("~")[0])/100; // 산정 고도 하한(100ft 단위)
		int areaThresholdMax = Integer.valueOf(areaThreshold.split("~")[1])/100; // 산정 고도 상한(100ft 단위)		
		
		List<double[]> areaPolygon = (List<double[]>) sectorData.get("areaPolygon");
				
		BoundXY boundXY = modelGridUtil.getBoundXY();
		
		int cropModelLeft = boundXY.getLeft();
		int cropModelRight = boundXY.getRight();
		int cropModelTop = modelGridUtil.getModelHeight() - boundXY.getTop() - 1;
		int cropModelBottom = modelGridUtil.getModelHeight() - boundXY.getBottom() - 1;
		
		// 폴리곤 내부 유효 격자 수
		int validCount = 0;
		
		// 산정 고도 범위에 해당하는 격자 수
		int thresholdCount = 0;
		
		try {
				
			double[] polygonExtent = getPolygonExtent(areaPolygon);
			
			PointXY polygonLeftTop = modelGridUtil.getPointXY(polygonExtent[2], polygonExtent[0]);
			int leftTopX = polygonLeftTop.getX();
			int leftTopY = modelGridUtil.getModelHeight() - polygonLeftTop.getY() - 1;
				
			PointXY polygonRightBottom = modelGridUtil.getPointXY(polygonExtent[3], polygonExtent[1]);
			int rightBottomX = polygonRightBottom.getX();
			int rightBottomY = modelGridUtil.getModelHeight() - polygonRightBottom.getY() - 1;
			
			for (int i = leftTopY; i <= rightBottomY; i++) {
				for (int j = leftTopX; j <= rightBottomX; j++) {				
					
					float value = koreaData[cropModelBottom - i][j - cropModelLeft];
					
					PointLonLat pointLonLat = modelGridUtil.getPointLonLat(j, modelGridUtil.getModelHeight() - 1 - i);
					
					boolean isInPolygon = isPointInPolygon(areaPolygon, pointLonLat.getLon(), pointLonLat.getLat());
					
					if (isInPolygon) {
						
						validCount++;
						
						if (value > areaThresholdMin && value <= areaThresholdMax) {
							thresholdCount++;
						}
					}
				}
			}		
			
			return (validCount > 0) ? (float) thresholdCount / validCount * 100 : 0; // 대류운 면적 비율(%) 반환
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0; // 예외 발생 시 0% 처리
	}
	
	/**
     * @param polygon 각 꼭짓점이 [경도, 위도]로 구성된 List<double[]>
     * @param x 검사할 점의 경도
     * @param y 검사할 점의 위도
     * @return 점이 폴리곤 내부에 있으면 true, 아니면 false
     */
    public static boolean isPointInPolygon(List<double[]> polygon, double x, double y) {
        int n = polygon.size();
        boolean inside = false;
        for (int i = 0, j = n - 1; i < n; j = i++) {
            double xi = polygon.get(i)[0], yi = polygon.get(i)[1];
            double xj = polygon.get(j)[0], yj = polygon.get(j)[1];

            boolean intersect = ((yi > y) != (yj > y)) &&
                    (x < (xj - xi) * (y - yi) / (yj - yi + 0.0) + xi);
            if (intersect) inside = !inside;
        }
        return inside;
    }
	
	 /**
     * polygon의 extent(경계) 정보를 반환합니다.
     * @param polygon 각 꼭짓점이 [경도, 위도]로 구성된 List<double[]>
     * @return [top, bottom, left, right] 순서의 double 배열
     */
    public static double[] getPolygonExtent(List<double[]> polygon) {
        if (polygon == null || polygon.isEmpty()) {
            throw new IllegalArgumentException("polygon이 비어있습니다.");
        }
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;

        for (double[] point : polygon) {
            double x = point[0]; // 경도
            double y = point[1]; // 위도

            if (y > top) top = y;
            if (y < bottom) bottom = y;
            if (x < left) left = x;
            if (x > right) right = x;
        }
        return new double[]{top, bottom, left, right};
    }
    
    private Font getFont(int fontSize, boolean isBold) {
    	
    	try {
        	
        	String fontFilePath = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res");
        	
        	File fontFile = null;
        	
			if (isBold) {
				fontFile = new File(fontFilePath + "/NanumSquareB.ttf");
			} else {
				fontFile = new File(fontFilePath + "/NanumSquareR.ttf");
			}

			// Font 객체 생성
        	
        	Font customFont = Font.createFont(Font.TRUETYPE_FONT, new FileInputStream(fontFile));

            // 요청한 크기로 파생(derive)
            Font sizedFont = customFont.deriveFont(Font.PLAIN, fontSize);
            
            return sizedFont;
    		
    	} catch (Exception e) {
    		
    	}
    	
    	return null;
    }
    
    public void process() {
    	
    	System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
    	
		if(!this.initialize()) {
			
			System.out.println("Error : AcimSectorTableGenerator.process -> initialize failed");
			return;
		}
		
		System.out.println("::: Start Get Acim Model File Map :::");
		
		// 처리 대상 KIM ACIM 모델 파일 목록 조회
		Map<String, List<NetcdfDataset>> acimModelFileMap = this.getAcimModelFileMap();
		
		for (Map.Entry<String, List<NetcdfDataset>> entry : acimModelFileMap.entrySet()) {

			String issuedTm = entry.getKey();
			List<NetcdfDataset> modelFileList = entry.getValue();
			
			// 발행시각별 섹터 테이블 이미지 생성
			boolean flag1 = this.generateSectorTable(issuedTm, modelFileList, 1);
			boolean flag2 = this.generateSectorTable(issuedTm, modelFileList, 2);
			
			if (flag1 && flag2) {
				
				for(int i=0 ; i<modelFileList.size() ; i++) {
					
					String modelFilePath = modelFileList.get(i).getLocation();
					
					String fileName = modelFilePath.substring(modelFilePath.lastIndexOf("/")+1);
					
					String fileDt = modelFilePath.substring(modelFilePath.lastIndexOf("_")+1).split("\\.")[0];
					
					String query = this.insertFileProcInfo.replaceAll("\\{fileDt\\}", fileDt)
														  .replaceAll("\\{fileName\\}", fileName)
														  .replaceAll("\\{filePath\\}", modelFilePath);
					
					this.dbManager.executeQuery(query);
				}
				
				this.dbManager.commit();

				
				System.out.println("-> Generate Sector Table Image Success: " + issuedTm);
			} else {
				System.out.println("-> Generate Sector Table Image Failed: " + issuedTm);
			}
			// NetCDF 파일 자원 해제
			this.destroyModelFileList(modelFileList);
		}
		
		this.destroy(); // 자원 해제
    }
    
    private boolean isRowSpanStart(List<Map<String, Object>> list, int rowIndex, String key) {

    	if (rowIndex == 0) {
    		return true;
    	}

    	String prev = list.get(rowIndex - 1).get(key).toString();
    	String curr = list.get(rowIndex).get(key).toString();

    	return !prev.equals(curr);
    }

    private int getRowSpanCount(List<Map<String, Object>> list, int rowIndex, String key) {

    	String value = list.get(rowIndex).get(key).toString();
    	int count = 1;

    	for (int i = rowIndex + 1; i < list.size(); i++) {
    		String next = list.get(i).get(key).toString();

    		if (!value.equals(next)) {
    			break;
    		}

    		count++;
    	}

    	return count;
    }

	public static void main(String[] args) {
		
		KimAcimSectorTableVer2Generator acim = new KimAcimSectorTableVer2Generator();
		acim.process(); // 프로세스 실행
		
    }

}
