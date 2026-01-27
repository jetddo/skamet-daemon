package kama.daemon.main;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
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

public class AcimSectorTableGenerator {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertParseComisDataInfo = 
			
			" INSERT INTO AAMI.PARSE_COMIS_DATA_HIS(FILE_DT, PROC_DT, FILE_NAME, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), SYSDATE, '{fileName}', 'ACIM_CNVT') "; 
	
	private final String selectParseComisDataInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.PARSE_COMIS_DATA_HIS						"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'ACIM_CNVT'									";
	
	String[] sectorNames = new String[] { "서해", "동해", "포항", "대구", "남해", "군산", "광주", "제주북부", "제주남부" };
	
	// 제주남부 빼고 다 180, 제주남부는 290임
	String[] sectorThresholds = new String[] { "180", "180", "180", "180", "180", "180", "180", "180", "290" };
	
	Color[] sectorBackgroundColors = new Color[] {
		new Color(223,230,247), // 서해
		new Color(223,230,247), // 동해
		new Color(223,230,247), // 포항
		new Color(223,230,247), // 대구
		new Color(223,230,247), // 남해
		new Color(223,230,187), // 군산
		new Color(223,230,187), // 광주
		new Color(223,230,187), // 제주북부
		new Color(223,230,187)  // 제주남부
	};
	
	String[] sectorCoords = new String[] {
		"38.0000|124.0000,38.0000|124.8500,38.3389|127.6644,37.0361|127.6644,36.9711|127.5611,37.1194|127.2311,37.0839|126.9375,36.9472|125.8061,36.9086|125.6000,36.3333|125.6000,36.3333|124.0000,38.0000|124.0000",
		"38.3389|127.6644,38.6333|128.3667,38.6333|129.8475,37.1194|129.8475,36.3364|129.8478,37.1194|128.6811,37.1194|127.8489,37.1111|127.6644,38.3389|127.6644",
		"36.9711|127.5611,37.0361|127.6644,37.1111|127.8489,37.1194|128.6811,36.3364|129.8478,36.1750|131.1675,35.8239|130.7236,35.4117|130.1700,35.4117|129.1569,35.9036|129.0811,36.9711|127.5611",
		"36.7386|127.1911,36.8383|127.3486,36.9711|127.5611,35.9036|129.0811,35.4117|129.1569,35.4117|130.1700,35.2089|129.8847,34.7886|129.3231,34.7231|129.2333,34.6667|129.1667,34.5031|129.0178,34.5031|128.4989,34.7653|128.4989,35.2103|128.4989,35.5031|127.8311,35.5178|127.8186,35.7531|127.6144,35.8636|127.6144,36.2031|127.6144,36.7386|127.1911",
		"35.5031|127.8311,35.2103|128.4989,34.5031|128.4989,34.5031|129.0178,33.7540|128.4517,33.6256|127.3314,33.9144|127.3314,34.2533|127.3314,35.1520|127.3144,35.2197|127.6478,35.5031|127.8311",
		"36.3333|124.0000,36.3333|125.6000,36.9086|125.6000,36.9472|125.8061,37.0839|126.9375,37.1194|127.2311,36.9711|127.5611,36.8383|127.3486,36.7386|127.1911,36.2031|127.6144,35.7531|127.6144,35.5031|127.8311,35.5031|126.6397,35.5031|124.0000,36.3333|124.0000",
		"35.5031|124.0000,35.5031|127.8311,35.2197|127.6478,35.1520|127.3144,34.2533|127.3314,34.0833|127.3314,33.8814|126.7325,33.8417|126.5672,33.8417|124.0000,35.5031|124.0000",
		"33.8417|124.0000,33.8417|126.5672,33.8814|126.7325,34.0833|127.3314,33.6256|127.3314,33.7540|128.4517,32.5000|127.5000,32.5000|126.8333,32.5397|126.2600,32.4799|125.9831,32.0414|124.0000,33.8417|124.0000",
		"32.0414|124.0000,32.4799|125.9831,32.5397|126.2600,32.5000|126.8333,30.0000|125.4167,30.0000|125.1983,30.0000|124.9533,30.0000|124.0000,31.6100|124.0000,32.0414|124.0000"			
	};
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private String storePath = null;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			storePath = this.config.getString("global.storePath.unix");
			
			//storePath = "\\\\172.26.56.115\\data_store";
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
	
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : ParseComisSanlimArmyData.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private List<Map<String, Object>> getSectorDataList() {		
		
		List<Map<String, Object>> sectorDataList = new ArrayList<Map<String, Object>>();
		
		for (int i = 0; i < sectorNames.length; i++) {
			
			Map<String, Object> sectorData = new HashMap<String, Object>();
			
			sectorData.put("sectorName", sectorNames[i]);
			sectorData.put("sectorBackgroundColor", sectorBackgroundColors[i]);			
			sectorData.put("sectorCode", String.format("%02d", i + 1)); // 01, 02, ..., 13
			sectorData.put("sectorThreshold", Integer.parseInt(sectorThresholds[i])); // 섹터 임계값
			
			String sectorCoord = sectorCoords[i];
			
			List<double[]> sectorPolygon = new ArrayList<double[]>();
			String[] coords = sectorCoord.split(",");
			for (String coord : coords) {
				String[] latLon = coord.split("\\|");
				if (latLon.length == 2) {
					double lat = Double.parseDouble(latLon[0]);
					double lon = Double.parseDouble(latLon[1]);
					sectorPolygon.add(new double[] { lon, lat }); // 경도, 위도 순서로 저장
				}
			}
			
			sectorData.put("sectorPolygon", sectorPolygon); // 섹터 폴리곤 좌표 저장
					
			sectorDataList.add(sectorData);
		}
		
		return sectorDataList;
	}
	
	/**
	 * ACIM 모델 파일을 반환합니다.
	 * 
	 * @return NetcdfDataset 배열
	 */
	private Map<String, List<NetcdfDataset>> getAcimModelFileMap() {
		
		Map<String, List<NetcdfDataset>> acimModelFileMap = new HashMap<String, List<NetcdfDataset>>();
		
		// 이부분에서 issuedTm 을 분석하여 모델데이터 파일 셋을 전달해야함
		// 실행되는 시간 시점으로부터 풀세트가 있는 Acim 모델 파일의 issuedTm 을 구한다
		// 아마 디비연동으로 수행하면 될듯?
		// 일단 issuedTm 을 구했다 치고 구현함
		// ACIM 데이터가 datastore 에 이미 저장되어있다고 가정하고 수행
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용		
		
		System.out.println("Store Path : " + storePath);
		
		Calendar cal = new GregorianCalendar();
		

		
		try {			
			//"2025071212"
			cal.setTime(new Date());
			cal.add(Calendar.HOUR_OF_DAY, -9-12);
			cal.add(Calendar.HOUR_OF_DAY, -cal.get(Calendar.HOUR_OF_DAY)%6); // 6시간 단위로 맞추기 위해 현재 시간을 6시간 단위로 내림 처리
			// 하루전꺼부터 체크
			
			String targetDtStr = sdf.format(cal.getTime());
			
			String query = this.selectParseComisDataInfoList.replaceAll("\\{targetDt\\}", targetDtStr);			
			
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

				// f00 부터 f39 까지 확인
				boolean allFilesPresent = true;
				
				for (int i = 0; i < 40; i+=3) {
					
					String expectedFileName = "amo_gdum_acim_cnvt_f" + String.format("%02d", i) + "_" + fileDt + ".nc";
					
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
				
				String targetDirStr = storePath + File.separator + "/ACIM_CNVT/" + sdf3.format(targetDt);
				
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
								
								return name.startsWith("amo_gdum_acim_cnvt_f") && name.endsWith(".nc");
							}							
						});
						
						// 각 파일에 대해 파일명이 f00~f39 까지 3시간 간격으로 전부 있는지 확인한다
						
						boolean allFilesPresent = true;
						
						for (int j = 0; j < 40; j+=3) {
							
							String expectedFileName = "amo_gdum_acim_cnvt_f" + String.format("%02d", j) + "_" + sdf2.format(targetDt) + ".nc";
								
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
							
							// 모든 파일이 존재하는 경우에만 모델 파일 리스트를 생성한다
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
										return Integer.compare(hour1, hour2); // 오름차순 정렬 
					                   
					                } catch (Exception e) {
					                    e.printStackTrace();
					                    return 0;
					                }
								}
								
							});
							
							// 모델 파일 리스트를 acimModelFileMap에 추가
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
	
	private boolean generateSectorTable(String issuedTmStr, List<NetcdfDataset> modelFileList) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yy-MM-dd");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH");
		
		try {
		
			String latPath = config.getString("gktg.coordinates.lat.path");
			String lonPath = config.getString("gktg.coordinates.lon.path");
	
			ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.GDPS, ModelGridUtil.Position.MIDDLE_CENTER, latPath, lonPath);
			modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(50, 20, 110, 150); // 한반도 영역 설정
			
			// 섹터 정보 및 폴리곤 좌표 설정
			List<Map<String, Object>> sectorDataList = this.getSectorDataList();
			
			int fcstHourLength = 39;
			int fcstHourInterval = 3; // 예측 시간 간격 (3시간 단위)
			int fcstHourSize = fcstHourLength / fcstHourInterval + 1; // 예측 시간 크기 (0시간 포함)
			
			int width = 1276;
			int height = 810;
			
	    	int tableLeftMargin = 60; // 왼쪽 여백
	    	int tableTopMargin = 200; // 위쪽 여백
	    	int tableRightMargin = 60; // 오른쪽 여백
	    	int tableBottomMargin = 150; // 아래쪽 여백
	    	
	    	int tableWidth = width - tableLeftMargin - tableRightMargin;
	    	int tableHeight = height - tableTopMargin - tableBottomMargin;
	    	
	    	int cellWidth = tableWidth / (fcstHourSize+1);
	    	int cellHeight = tableHeight / (sectorDataList.size()+2);
	    	
	    	int extraWidth = tableWidth % (fcstHourSize+1); // 나머지 계산
	    	int extraHeight = tableHeight % (sectorDataList.size()+2); // 나머지 계산
	    	
	    	int fontSize1 = (int) (cellHeight * 0.4); // 셀 높이에 비례하여 폰트 크기 조정
	    	int fontSize2 = (int) (cellHeight * 0.35); // 셀 높이에 비례하여 폰트 크기 조정
	    	int fontSize3 = (int) (cellHeight * 0.40); // 셀 높이에 비례하여 폰트 크기 조정
	    	
	    	Font font1 = this.getFont(fontSize1, true);
	    	Font font2 = this.getFont(fontSize2, true);
	    	Font font3 = this.getFont(fontSize3, true);
	    	
//	    	System.out.println("테이블 크기: " + tableWidth + "x" + tableHeight);
//	    	System.out.println("셀 크기: " + cellWidth + "x" + cellHeight);
//	    	System.out.println("섹터 개수: " + sectorDataList.size());
//	    	System.out.println("예측 시간 크기: " + fcstHourSize);
			
	        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
	        Graphics2D g = image.createGraphics();
	        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
	        g.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
	        
	        // 배경 설정
	        g.setColor(Color.WHITE);
	        g.fillRect(0, 0, width, height);
	     	
			Calendar cal = new GregorianCalendar();
			Date issuedTm = sdf.parse(issuedTmStr);
	        
	        this.createFrameInfo(g, width, height, issuedTm); // 상단 프레임 정보 생성);

			System.out.println("\n::: Start Generate Sector Table :::");
					
			System.out.println("-> Issued Time: " + sdf3.format(issuedTm) + ", Model File Count: " + modelFileList.size());
			
			int currentDay = 0;
			boolean dayChanged = false;
		 
	        for (int j = 0; j < fcstHourSize+1; j++) {  
	        	
	        	NetcdfDataset ncFile = null;
	        	Float[][] koreaData = null; // 한반도 영역 데이터
	        	
	        	// 여기서 한반도의 value 를 미리 추출해둔다
	        	
				if (j > 0) {
					
					cal.setTime(issuedTm);
					
					cal.add(Calendar.HOUR_OF_DAY, fcstHourInterval * (j-1));
					
					int newDay = cal.get(Calendar.DAY_OF_MONTH);
					
					if(currentDay == 0) {
						currentDay = newDay;
					} else {
					
						if(currentDay != newDay) {
							dayChanged = true;
							currentDay = newDay;
						}
					}
					
					ncFile = modelFileList.get(j - 1); // 첫 번째 열은 예측 시간 이름이 없음
					
					Variable var = ncFile.findVariable("CCT"); // CLTOP 변수 찾기
					
					koreaData = this.getAcimModelKoreaData(ncFile, modelGridUtil, var); // 한반도 영역 데이터 추출				
				}
	        	
				for (int i = 0; i < sectorDataList.size()+2; i++) {
			        	
		        	Map<String, Object> sectorData = null;
		        	
					if (i > 1) {
						sectorData = sectorDataList.get(i - 2); // 첫 번째 행은 섹터 이름이 없음
					}
	            	
	                int x = j * cellWidth + tableLeftMargin + Math.min(j, extraWidth); // 나머지 분배
	                int y = i * cellHeight + tableTopMargin + Math.min(i, extraHeight); // 나머지 분배
	
	                int currentCellWidth = cellWidth;
	                int currentCellHeight = cellHeight;
	                
	                if (j < extraWidth) {
	                    currentCellWidth += 1; // 나머지를 앞쪽 셀에 분배
	                }
	                
					if (i < extraHeight) {
						currentCellHeight += 1; // 나머지를 위쪽 셀에 분배
					}
	            	
	            	if(i == 0) {
						
						g.setColor(new Color(255,231,216)); // 배경색 설정
						g.fillRect(x, y, currentCellWidth, currentCellHeight); // 셀 배경색 채우기
	            	
						// 첫 번째 행은 예측 시간 표시
						if (j == 0) {
							
							g.setColor(Color.BLACK);
							g.setFont(font1);
							
							this.setCellText("DATE", g, x-4, y, cellWidth, cellHeight, font1);
							
						}
						
					} else if(i == 1) {
						
						g.setColor(new Color(255,231,216)); // 배경색 설정
						g.fillRect(x, y, currentCellWidth, currentCellHeight); // 셀 배경색 채우기
						
						// 첫 번째 행은 예측 시간 표시
						if (j == 0) {
							
							g.setColor(Color.BLACK);
							g.setFont(font1);
							
							this.setCellText("TIME", g, x-1, y - font1.getSize()/2, cellWidth, cellHeight, font1);
							this.setCellText("(UTC)", g, x+3, y + font2.getSize()/2+2, cellWidth, cellHeight, font2);
							
						} else {
							
							// 첫 번째 행의 나머지 셀은 예측 시간 표시
							String fcstHourText = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY));
	
							g.setColor(Color.BLACK);
							g.setFont(font1);
	
							this.setCellText(fcstHourText, g, x, y, currentCellWidth, currentCellHeight, font1);						
						}
						
					} else {
						
						// 나머지 행은 섹터 이름 표시
						if (j == 0) {
							
							String sectorName = sectorData.get("sectorName").toString();
							Color sectorBackgroundColor = (Color) sectorData.get("sectorBackgroundColor");
							
							g.setColor(sectorBackgroundColor); // 배경색 설정
							g.fillRect(x, y, currentCellWidth, currentCellHeight); // 셀 배경색 채우기
							
							g.setColor(Color.BLACK);
							g.setFont(font2);
							this.setCellText(sectorName, g, x, y, currentCellWidth, currentCellHeight, font2);
							
						} else {
							
							// 이제 여기서 모델자료 분석해서 색칠하면 됨
							
							float thresholdRatio = this.getAcimModelAvgDataInSector(sectorData, modelGridUtil, koreaData);
							
							Color thresholdColor = null;
							
							if (thresholdRatio == 0) {
								thresholdColor = new Color(255, 255, 255); // 0인 경우 흰색
							} else if (thresholdRatio > 0 && thresholdRatio < 3) {
								thresholdColor = new Color(128, 228, 16); // ~3
							} else if (thresholdRatio >=3 && thresholdRatio < 10) {
								thresholdColor = new Color(0, 183, 80); // 3~10
							} else if (thresholdRatio >= 10 && thresholdRatio < 20) {
								thresholdColor = new Color(255, 252, 0); // 10~20
							} else if (thresholdRatio >= 20 && thresholdRatio < 30) {
								thresholdColor = new Color(255, 130, 0); // 20~30
							} else {
								thresholdColor = new Color(255, 0, 0); // 30~
							}
							
							if (thresholdColor != null) {
								g.setColor(thresholdColor); // 배경색 설정
								g.fillRect(x, y, currentCellWidth, currentCellHeight); // 셀 배경색 채우기
							}
						}
					}
	            	
	            	if(i == 0 && j > 1) {
	            		
	            		if(dayChanged || j == fcstHourSize) {
	            			
	            			int h = 0;
	            			Date d = cal.getTime();
	            			
	            			if(dayChanged) {
		            			
		            			g.setColor(Color.BLACK);
								g.setStroke(new BasicStroke(1)); // 선 굵기 5px
				                g.drawRect(x, y, currentCellWidth, currentCellHeight);	
		            			
		            			dayChanged = false;
		            			
		            			// 날짜가 바뀐 경우 h 는 최대 24
		            			h = Math.min(24, (int)((d.getTime() - issuedTm.getTime()) / 1000 / 60 / 60));
		            			
		            			d = new Date(d.getTime() - 1000*60);
		            			
	            			} else {
	            				h = cal.get(Calendar.HOUR_OF_DAY)-3;
	            			}
		
	            			g.setColor(Color.BLACK);
							g.setFont(font2);
							this.setCellText(sdf2.format(d), g, x - h*currentCellWidth/fcstHourInterval/2, y, 0, currentCellHeight, font1);
	            		}
	            		
	            	} else {
	            		
						g.setColor(Color.BLACK);
						g.setStroke(new BasicStroke(1)); // 선 굵기 5px
		                g.drawRect(x, y, currentCellWidth, currentCellHeight);	
	            	}
	            }
	        }
	    	
	        // 셀 테두리 그리기
	        g.setStroke(new BasicStroke(3)); // 선 굵기 5px
	    	g.setColor(Color.BLACK);
	    	
	        g.drawRect(tableLeftMargin, tableTopMargin, width - tableLeftMargin - tableRightMargin, height - tableTopMargin - tableBottomMargin);  
	        
	        this.createFooterInfo(g, width, height, tableLeftMargin, tableTopMargin, tableBottomMargin, issuedTm, sectorNames, sectorThresholds); // 기준고도 정보 생성
	        this.createLegend(g, width, height, tableRightMargin, tableBottomMargin, font3); // 범례 생성
	        
	        g.dispose(); // Graphics2D 객체 자원 해제
	        
        	String y = issuedTmStr.substring(0, 4);
        	String m = issuedTmStr.substring(4, 6);
        	String d = issuedTmStr.substring(6, 8);
        	String h = issuedTmStr.substring(8, 10);
        	
        	String imgFileDirPath = storePath+"/CDM_RPT/CLD/"+y+"/"+m+"/"+d;
        	
        	//String imgFileDirPath = "C:/data/datastore/CDM_RPT/CLD/"+y+"/"+m+"/"+d;
        	
            File imgFileDir = new File(imgFileDirPath);
            
            if(!imgFileDir.exists()) {
            	imgFileDir.mkdirs();
            }
            
            File imgFile = new File(imgFileDirPath + "/cld_fct_"+issuedTmStr+".png");
            
            ImageIO.write(image, "png", imgFile);
            
            System.out.println("-> Create ACIM Sector Table Image: " + imgFile.getAbsolutePath());
            
        } catch (Exception e) {
        	
        	e.printStackTrace();
        	return false;
        	
        } finally {
            
            // 모델 파일 리스트 자원 해제
            this.destroyModelFileList(modelFileList);        	
        }
        
        return true;
	}
	
	private void createFooterInfo(Graphics2D g, int width, int height, int tableLeftMargin, int tableTopMargin, int tableBottomMargin, Date issuedTm, String[] sectorNames, String[] sectorThresholds) {
		
		
		// sectorNames 와 sectorThresholds 를 이용하여 기준고도 정보를 생성합니다.
		
		// 아래와 같은 형태로 만들어야함
		// 18000ft: 서해, 동해, 포항, 대구, 남해, 군산, 광주, 제주북부
		// 29000ft: 제주남부
		
		List<String> infoStringList = new ArrayList<String>();
		
		// 고도가 같은 것은 합쳐져야함
		
		Map<String, List<String>> thresholdMap = new HashMap<>();
		
		for (int i = 0; i < sectorNames.length; i++) {
			String threshold = sectorThresholds[i];
			String sectorName = sectorNames[i];

			if (!thresholdMap.containsKey(threshold)) {
				thresholdMap.put(threshold, new ArrayList<String>());
			}
			thresholdMap.get(threshold).add(sectorName);
		}
		
		for (Map.Entry<String, List<String>> entry : thresholdMap.entrySet()) {
			String threshold = entry.getKey();
			List<String> sectors = entry.getValue();
			String infoString = threshold + "00ft: " + String.join(", ", sectors);
			infoStringList.add(infoString);
		}
		
		int infoTopMargin = (height - tableBottomMargin) + 30;
		
		Font font = this.getFont(18, true);
		
        g.setFont(font);
        g.setColor(Color.BLACK);
        
        g.drawString("기준고도:", tableLeftMargin, infoTopMargin + font.getSize()); // 범례 텍스트 위치 조정);
        
		for (int i = 0; i < infoStringList.size(); i++) {
			String infoString = infoStringList.get(i);
			g.drawString(infoString, tableLeftMargin, infoTopMargin + (font.getSize() + 5) * (i + 2)); // 범례 텍스트 위치 조정);
		}
	}
	
	private void createFrameInfo(Graphics2D g, int width, int height, Date issuedTm) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy년 MM월 dd일 HHmm");
		
		// 문서 형태처럼 전체를 감싸는 굵은 선을 그린다
		
		int leftMargin = 20;
		int topMargin = 20;
		int rightMargin = 20;
		int bottomMargin = 20;		
        
        int logoSize = 140;
        int titleHeight = 80;
        
        try {
        	
            String logoFilePath = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res") + File.separator + "amo_logo.png";
            
            BufferedImage logoImg = ImageIO.read(new File(logoFilePath));
            
            g.drawImage(logoImg, leftMargin+10, topMargin+10, logoSize-20, logoSize-20, null);

        	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		g.setColor(Color.BLACK);
		g.setStroke(new BasicStroke(2)); // 선 굵기 5px
        g.drawRect(leftMargin, topMargin, width - leftMargin - rightMargin, height - topMargin - bottomMargin);
        
        g.setStroke(new BasicStroke(1)); // 선 굵기 5px
        g.drawLine(leftMargin, topMargin + logoSize, width - rightMargin, topMargin + logoSize); // 상단 가로선
        
        // 왼쪽 위에 로고가 들어갈 작은 박스를 그림
        
        g.drawRect(leftMargin, topMargin, logoSize, logoSize);
        
        g.drawLine(leftMargin + logoSize, topMargin + titleHeight, width - rightMargin, topMargin + 80); // 상단 가로선2
		
        Font titleFont = this.getFont(40, true);
        Font infoFont = this.getFont(20, true);
        g.setFont(titleFont);
        g.setColor(Color.BLACK);
        
        String titleText = "대류운 정보";
        
        g.drawString(titleText, this.getCellTextLeftMargin(titleText, width, titleFont), topMargin + titleFont.getSize() + 10); // 수직 중앙 정렬
        
        g.setFont(infoFont);
        g.drawString("발표시각: " + sdf.format(issuedTm) + "UTC", leftMargin + logoSize + 15, topMargin + titleHeight + infoFont.getSize() + 15); // 범례 텍스트 위치 조정);
        
        String agency = "항공기상청 예보과";
        g.drawString(agency, width - agency.length()*infoFont.getSize(), topMargin + titleHeight + infoFont.getSize() + 15); // 범례 텍스트 위치 조정);
		
	}
	
	private void createLegend(Graphics2D g, int width, int height, int tableRightMargin, int tableBottomMargin, Font font) {
		
		// 색상 셀과 텍스트 설정
		String[] thresholds = { "~3", "3~10", "10~20", "20~30", "30~" };
		Color[] colors = { new Color(128,228,16), new Color(0,183,80), new Color(255,252,0), new Color(255,130,0), new Color(255,0,0) };
		
		int legendWidth = width / 3; // 전체 너비에서 여백을 뺀 너비
		int legendHeight = 25; // 고정 높이
		
		int legendLeftMargin = width - legendWidth - tableRightMargin; // 왼쪽 여백
		int legendTopMargin = (height - tableBottomMargin) + 30; // 위쪽 여백

		int cellWidth = legendWidth / thresholds.length; // 셀 너비 (3개의 색상 셀)
		int cellHeight = legendHeight; // 셀 높이 (고정 높이)

		g.setColor(Color.WHITE);
		g.fillRect(legendLeftMargin, legendTopMargin, legendWidth, legendHeight); // 배경색 채우기
		
        g.setFont(font);
        g.setColor(Color.BLACK);
        
        String legendTitle = "범례: ";
        
        g.drawString("범례: ", legendLeftMargin - legendTitle.length()*font.getSize()/3*2, legendTopMargin + font.getSize()); // 범례 텍스트 위치 조정);

		for (int i = 0; i < thresholds.length; i++) {
			
			int x = legendLeftMargin + i * cellWidth;
			g.setColor(colors[i]);
			g.fillRect(x, legendTopMargin, cellWidth, cellHeight); // 색상 셀 채우기
			
			g.setColor(Color.BLACK);
			this.setCellText(thresholds[i], g, x-2, legendTopMargin-2, cellWidth, cellHeight, font); // 텍스트 가운데 정렬
			
			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(1)); // 선 굵기 1px
			g.drawRect(x, legendTopMargin, cellWidth, cellHeight); // 테두리 그리기
		}

		g.setColor(Color.BLACK);
		g.setStroke(new BasicStroke(2)); // 선 굵기 1px
		g.drawRect(legendLeftMargin, legendTopMargin, legendWidth, legendHeight); // 테두리 그리기
	}
	
	private int getCellTextLeftMargin(String text, int cellWidth, Font font) {
		
		int textWidth = text.length() * font.getSize();
		
		if (this.isKorean(text)) {
			textWidth = text.length() * font.getSize(); // 한글은 대략적인 문자 너비 계산
		} else {
			textWidth = text.length() * font.getSize() / 5 * 3; // 영어는 대략적인 문자 너비 계산
		}
		
		return (cellWidth - textWidth) / 2; // 가운데 정렬을 위한 여백 계산
	}
	
	private int getCellTextTopMargin(int cellHeight, Font font) {
		return (cellHeight - font.getSize()) / 2; // 수직 중앙 정렬을 위한 여백 계산
	}
	
	private void setCellText(String text, Graphics2D g, int x, int y, int cellWidth, int cellHeight, Font font) {
		
		g.setFont(font);
		int fontLeftMargin = getCellTextLeftMargin(text, cellWidth, font);
		int fontTopMargin = getCellTextTopMargin(cellHeight, font);
		
		// 폰트의 높이만큼 더 더해줘야한다
		// 수직 중앙 정렬을 위해 폰트의 높이만큼 더해준다		 
		
		g.drawString(text, x + fontLeftMargin+2, y + fontTopMargin + font.getSize()-2); // 수직 중앙 정렬
	}
	
	private boolean isKorean(String text) {
        for (char c : text.toCharArray()) {
            if (c >= '\uAC00' && c <= '\uD7A3') {
                return true; // 한글이 포함된 경우
            }
        }
        return false; // 한글이 없는 경우
    }
	
	private Float[][] getAcimModelKoreaData(NetcdfDataset ncFile, ModelGridUtil modelGridUtil, Variable var) {

		try {

			BoundXY boundXY = modelGridUtil.getBoundXY();
			
			int rows = modelGridUtil.getRows();
			int cols = modelGridUtil.getCols();
			
			List<Range> rangeList = new ArrayList<Range>();
			rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
			Float[][] values = GridCalcUtil.convertStorageToValuesReverse(var.read(rangeList).getStorage(), rows, cols);

			return values;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private float getAcimModelAvgDataInSector(Map<String, Object> sectorData, ModelGridUtil modelGridUtil, Float[][] koreaData) {
				
		Integer sectorThreshold = (Integer) sectorData.get("sectorThreshold"); // 섹터 임계값
		
		List<double[]> sectorPolygon = (List<double[]>) sectorData.get("sectorPolygon");
				
		BoundXY boundXY = modelGridUtil.getBoundXY();
		
		int cropModelLeft = boundXY.getLeft();
		int cropModelRight = boundXY.getRight();
		int cropModelTop = modelGridUtil.getModelHeight() - boundXY.getTop() - 1;
		int cropModelBottom = modelGridUtil.getModelHeight() - boundXY.getBottom() - 1;
		
		// 유효격자수
		int validCount = 0;
		
		// 임계값 이상 격자수
		int thresholdCount = 0;
		
		try {
				
			double[] polygonExtent = getPolygonExtent(sectorPolygon);
			
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
					
					boolean isInPolygon = isPointInPolygon(sectorPolygon, pointLonLat.getLon(), pointLonLat.getLat());
					
					if (isInPolygon) {
						
						validCount++;
						
						if(value >= sectorThreshold) { // 임계값 290K 이상) {
							thresholdCount++;							
						}
					}
				}
			}		
			
			return (validCount > 0) ? (float) thresholdCount / validCount * 100 : 0; // 퍼센트로 반환
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0; // 예외 발생 시 0 반환
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

			// 2. Font 객체 생성
        	
        	Font customFont = Font.createFont(Font.TRUETYPE_FONT, new FileInputStream(fontFile));

            // 3. 원하는 크기로 파생 (derive)
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
		
		// ACIM 모델 파일 리스트 가져오기
		Map<String, List<NetcdfDataset>> acimModelFileMap = this.getAcimModelFileMap();
		
		for (Map.Entry<String, List<NetcdfDataset>> entry : acimModelFileMap.entrySet()) {

			String issuedTm = entry.getKey();
			List<NetcdfDataset> modelFileList = entry.getValue();
			
			// ACIM 섹터 테이블 생성
			if(this.generateSectorTable(issuedTm, modelFileList)) {
				
				for(int i=0 ; i<modelFileList.size() ; i++) {
					
					String modelFilePath = modelFileList.get(i).getLocation();
					
					String fileName = modelFilePath.substring(modelFilePath.lastIndexOf("/")+1);
					
					String fileDt = modelFilePath.substring(modelFilePath.lastIndexOf("_")+1).split("\\.")[0];
					
					String query = this.insertParseComisDataInfo.replaceAll("\\{fileDt\\}", fileDt)
																.replaceAll("\\{fileName\\}", fileName);
					
					this.dbManager.executeQuery(query);
				}
			}

			// 모델 파일 리스트 자원 해제
			this.destroyModelFileList(modelFileList);
		}
		
		this.destroy(); // 자원 해제
    }

	public static void main(String[] args) {
		
		AcimSectorTableGenerator acim = new AcimSectorTableGenerator();
		acim.process(); // 프로세스 실행
		
    }

}
