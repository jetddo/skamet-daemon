package kama.daemon.main;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

public class DfsTideFcstTableGenerator {
	
	private String[] regionInfoList = new String[] {"울산", "김해"};
	private String[] dfsPointInfoList = new String[] {"송정동|0,0", "대저2동|0,0"};
	private String[] tidePointInfoList = new String[] {"울산항|58", "다대포|115"};
	
	private static final int INVALID_VALUE = -999;
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private String getTideInfoQuery = 
			
			" SELECT                                                         			"+
			"   STA_UID AS STA_UID,                                          			"+
			"   TO_CHAR(FCST_DATE, 'YYYYMMDD') AS FCST_DATE,                 			"+
			"   TO_CHAR(MIN_TIDE_DATE_1, 'YYYYMMDDHH24MI') AS MIN_TIDE_DATE_1, 			"+
			"   TO_CHAR(MIN_TIDE_DATE_2, 'YYYYMMDDHH24MI') AS MIN_TIDE_DATE_2, 			"+
			"   TO_CHAR(MAX_TIDE_DATE_1, 'YYYYMMDDHH24MI') AS MAX_TIDE_DATE_1, 			"+
			"   TO_CHAR(MAX_TIDE_DATE_2, 'YYYYMMDDHH24MI') AS MAX_TIDE_DATE_2, 			"+
			"   MIN_TIDE_1 AS MIN_TIDE_1,                                    			"+
			"   MIN_TIDE_2 AS MIN_TIDE_2,                                    			"+
			"   MAX_TIDE_1 AS MAX_TIDE_1,                                    			"+
			"   MAX_TIDE_2 AS MAX_TIDE_2                                     			"+
			" FROM KHOA_TIDE_INFO                                            			"+
			" WHERE 1=1                                                      			"+
			" AND FCST_DATE >= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')-1        		"+
			" AND FCST_DATE <= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')+{fcstDaySize}	"+
			" AND STA_UID = {tideUid}                                          			";
	
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
			
			System.out.println("Error : DfsTideFcstTableGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private boolean generateDfsTideTable(String issuedTmStr, int fcstDaySize) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH");
		
		try {
			
			int width = 550*fcstDaySize + 150; // 예보 일수에 따라 가로 길이 조정;
			int height = 400;	    	
	    	
	        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
	        Graphics2D g = image.createGraphics();
	        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
	        g.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
	        
	        // 배경 설정
	        g.setColor(Color.WHITE);
	        g.fillRect(0, 0, width, height);
	     	
			Date issuedTm = sdf.parse(issuedTmStr);

	        this.createDfsTideTable(g, width, height, issuedTm, fcstDaySize); // 예보 테이블 정보 생성;
	        
			System.out.println("\n::: Start Generate Sector Table :::");
					
			System.out.println("-> Issued Time: " + sdf2.format(issuedTm));
	        
	        g.dispose(); // Graphics2D 객체 자원 해제
	        
            
            File imgFile = new File( "F:/data/test.png");
            
            ImageIO.write(image, "png", imgFile);
            
            System.out.println("-> Create ACIM Sector Table Image: " + imgFile.getAbsolutePath());
            
        } catch (Exception e) {
        	
        	e.printStackTrace();
        	return false;
        	
        } finally {
            
        }
        
        return true;
	}
	
	private void createDfsTideTable(Graphics2D g, int width, int height, Date issuedTm, int fcstDaySize) throws Exception {
		
		SimpleDateFormat tideBaseFormat = new SimpleDateFormat("yyyyMMddHHmm");
		SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
		SimpleDateFormat dateHeaderFormat = new SimpleDateFormat("MM월 dd일 (E)");
		
		Calendar cal = new GregorianCalendar();
		
		// ===== 전체 테이블 여백 =====
		int marginLeft = 10;
		int marginTop = 10;
		int marginRight = 10;
		int marginBottom = 10;

		// ===== 전체 테이블 크기 =====
		int tableX = marginLeft;
		int tableY = marginTop;
		int tableW = width - marginLeft - marginRight;
		int tableH = height - marginTop - marginBottom;

		// ===== 기본 설정 =====
		int regionCount = this.regionInfoList.length;
		int hourInterval = 3;
		int cellsPerDay = 24 / hourInterval;

		// ===== 좌/우 고정 컬럼 =====
		int regionColW = 50;
		int elementColW = 100;
		int remarkColW = 100;

		// ===== 헤더 영역 =====
		int dateHeaderH = 35;
		int timeHeaderH = 35;
		int headerH = dateHeaderH + timeHeaderH;

		// ===== 예보 영역 =====
		int fcstAreaX = marginLeft + regionColW + elementColW;
		int fcstAreaRightX = width - marginRight - remarkColW;
		int fcstAreaW = fcstAreaRightX - fcstAreaX;

		// 기존 int 나눗셈 제거
		double dayColW = fcstAreaW / (double) fcstDaySize;
		double timeCellW = fcstAreaW / (double)(fcstDaySize * cellsPerDay);

		// ===== 본문 영역 =====
		int bodyY = marginTop + headerH;
		int regionRowH = (tableH - headerH) / regionCount;

		// ===== 지역 row 내부 영역 =====
		int rainRowH = (int)(regionRowH / 3.5);
		int tideRowH = regionRowH - rainRowH;
		int tideTypeRowH = tideRowH / 10;
		int tideGraphH = (int)(tideRowH * 0.7);

		// ===== 폰트 =====
		int headerFontSize = (int)(dateHeaderH * 0.4);
		int dateFontSize = (int)(dateHeaderH * 0.37);
		int timeFontSize = (int)(dateHeaderH * 0.35);

		Font headerFont = this.getFont(headerFontSize, true);
		Font dateFont = this.getFont(dateFontSize, true);
		Font timeFont = this.getFont(timeFontSize, true);

		// ===== 헤더 배경 =====
		g.setColor(new Color(232, 232, 248));
		g.fillRect(tableX, tableY, tableW, headerH);

		g.setStroke(new BasicStroke(2));
		g.setColor(Color.BLACK);

		// ===== 좌측 헤더 =====
		g.drawRect(marginLeft, marginTop, regionColW, headerH);
		this.setCellText("구분", g, marginLeft, marginTop, regionColW, headerH, headerFont);

		g.drawRect(marginLeft + regionColW, marginTop, elementColW, headerH);
		this.setCellText("기상요소", g, marginLeft + regionColW, marginTop, elementColW, headerH, headerFont);

		// ===== 비고 헤더 =====
		g.drawRect(width - marginRight - remarkColW, marginTop, remarkColW, headerH);

		// ===== 전체 헤더 테두리 =====
		g.drawRect(tableX, tableY, tableW, headerH);

		// ===== 지역별 좌측 컬럼 / 기상요소 컬럼 =====
		for (int regionIndex = 0; regionIndex < regionCount; regionIndex++) {
			
			String region = regionInfoList[regionIndex];
			String dfsPointName = dfsPointInfoList[regionIndex].split("\\|")[0];
			String tidePointName = tidePointInfoList[regionIndex].split("\\|")[0];

			int rowY = bodyY + regionIndex * regionRowH;

			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(2));

			// 구분 컬럼
			g.drawRect(marginLeft, rowY, regionColW, regionRowH);

			g.setColor(new Color(0, 0, 255));
			this.setCellText(region, g, marginLeft, rowY, regionColW, regionRowH, headerFont);

			g.setColor(Color.BLACK);

			// 기상요소 텍스트
			this.setCellText("강수량", g, marginLeft + regionColW, rowY - headerFont.getSize() + headerFont.getSize() / 3, elementColW, rainRowH, headerFont);
			this.setCellText("(" + dfsPointName + ")", g, marginLeft + regionColW + headerFont.getSize() / 3, rowY + headerFont.getSize() / 2, elementColW, rainRowH, headerFont);

			this.setCellText("조위", g, marginLeft + regionColW, rowY + rainRowH - headerFont.getSize(), elementColW, tideRowH, headerFont);
			this.setCellText("(" + tidePointName + ")", g, marginLeft + regionColW + headerFont.getSize() / 3, rowY + rainRowH + headerFont.getSize(), elementColW, tideRowH, headerFont);

			// 기상요소 컬럼 테두리
			g.setStroke(new BasicStroke(2));
			g.drawRect(marginLeft + regionColW, rowY, elementColW, regionRowH);

			// 강수량 / 조위 구분선
			g.setStroke(new BasicStroke(1));
			g.drawLine(marginLeft + regionColW, rowY + rainRowH, width - marginRight, rowY + rainRowH);

			// 조위 그래프 / 대조기·소조기 영역 구분선
			g.drawLine(
				fcstAreaX,
				rowY + rainRowH + tideRowH - tideTypeRowH,
				fcstAreaRightX,
				rowY + rainRowH + tideRowH - tideTypeRowH
			);
		}

		// ===== 지역 row 사이 굵은 구분선 =====
		g.setStroke(new BasicStroke(2));
		for (int regionIndex = 1; regionIndex < regionCount; regionIndex++) {
			int y = bodyY + regionIndex * regionRowH;
			g.drawLine(fcstAreaX, y, width - marginRight, y);
		}

		// ===== 날짜 / 시간 헤더 =====
		cal.setTime(issuedTm);

		g.setStroke(new BasicStroke(1));
		g.setColor(Color.BLACK);

		for (int dayIndex = 0; dayIndex < fcstDaySize; dayIndex++) {
			
			int dayX = (int)Math.round(fcstAreaX + dayIndex * dayColW);
			int nextDayX = (int)Math.round(fcstAreaX + (dayIndex + 1) * dayColW);
			int dayW = nextDayX - dayX;

			String dayText = dateHeaderFormat.format(cal.getTime());

			this.setCellText(dayText, g, dayX, marginTop, dayW, dateHeaderH, dateFont);

			// 일 구분 세로선
			g.drawLine(dayX, marginTop, dayX, marginTop + headerH);

			for (int timeIndex = 0; timeIndex < cellsPerDay; timeIndex++) {
				
				int cellIndex = dayIndex * cellsPerDay + timeIndex;

				int cellX = (int)Math.round(fcstAreaX + cellIndex * timeCellW);
				int nextCellX = (int)Math.round(fcstAreaX + (cellIndex + 1) * timeCellW);
				int cellW = nextCellX - cellX;

				String startHourText = hourFormat.format(cal.getTime());
				cal.add(Calendar.HOUR_OF_DAY, hourInterval);
				String endHourText = hourFormat.format(cal.getTime());

				if ("00".equals(endHourText)) {
					endHourText = "24";
				}

				this.setCellText(
					startHourText + "~" + endHourText,
					g,
					cellX,
					marginTop + dateHeaderH,
					cellW,
					timeHeaderH,
					timeFont
				);

				// 시간 헤더 세로선
				g.drawLine(cellX, marginTop + dateHeaderH, cellX, marginTop + headerH);

				// 본문 시간 세로선
				for (int regionIndex = 0; regionIndex < regionCount; regionIndex++) {
					
					int rowY = bodyY + regionIndex * regionRowH;

					g.drawLine(cellX, rowY, cellX, rowY + rainRowH);
					g.drawLine(cellX, rowY + rainRowH, cellX, rowY + regionRowH - tideTypeRowH);
				}
			}
		}

		// 마지막 날짜 끝 세로선
		g.drawLine(fcstAreaRightX, marginTop, fcstAreaRightX, marginTop + headerH);

		// 날짜 / 시간 가로 구분선
		g.drawLine(fcstAreaX, marginTop + dateHeaderH, fcstAreaRightX, marginTop + dateHeaderH);

		// 비고란 왼쪽 세로선
		g.drawLine(fcstAreaRightX, marginTop, fcstAreaRightX, height - marginBottom);

		// 전체 테두리
		g.setStroke(new BasicStroke(2));
		g.drawRect(tableX, tableY, tableW, tableH);

		g.setStroke(new BasicStroke(1));

		// ===== 조위 그래프 =====
		for (int regionIndex = 0; regionIndex < tidePointInfoList.length; regionIndex++) {
			
			String[] tidePointInfo = tidePointInfoList[regionIndex].split("\\|");
			int tidePointUid = Integer.parseInt(tidePointInfo[1]);

			Map<String, Object> tideDrawInfo = getTideDrawInfo(tideBaseFormat.format(issuedTm), fcstDaySize, tidePointUid);

			printTideDrawInfo(tideDrawInfo);

			System.out.println("\n::: Tide Draw Info :::");

			drawTide(
				g,
				tideDrawInfo,
				fcstDaySize,
				fcstAreaW,
				tideGraphH,
				fcstAreaX,
				bodyY + regionIndex * regionRowH + rainRowH
			);
		}
	}
	
	
	
	private Map<String, Object> getTideDrawInfo(String issuedTmStr, int fcstDaySize, int tideUid) throws Exception {
		
		String query = getTideInfoQuery.replaceAll("\\{issuedTmStr\\}", issuedTmStr)
									   .replaceAll("\\{fcstDaySize\\}", fcstDaySize + "")
									   .replaceAll("\\{tideUid\\}", tideUid + "");

		List<Map<String, Object>> tideInfoList = new ArrayList<Map<String, Object>>();
		
		ResultSet resultSet = dbManager.executeQuery(query);

		while (resultSet.next()) {
			Map<String, Object> tideInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);
			tideInfoList.add(tideInfo);
		}

		Map<String, Object> tideDrawInfo = new HashMap<String, Object>();

		if (tideInfoList.size() < 3) {
			tideDrawInfo.put("maxTideList", new int[0]);
			tideDrawInfo.put("maxTideXList", new int[0]);
			tideDrawInfo.put("minTideList", new int[0]);
			tideDrawInfo.put("minTideXList", new int[0]);
			tideDrawInfo.put("prevTide", INVALID_VALUE);
			tideDrawInfo.put("prevTideX", INVALID_VALUE);
			tideDrawInfo.put("nextTide", INVALID_VALUE);
			tideDrawInfo.put("nextTideX", INVALID_VALUE);
			tideDrawInfo.put("debugTidePointList", new ArrayList<Map<String, Object>>());
			return tideDrawInfo;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		Date baseDate = sdf.parse(issuedTmStr + "00");

		String[] tideValueKeys = {"minTide1", "maxTide1", "minTide2", "maxTide2"};
		String[] tideDateKeys = {"minTideDate1", "maxTideDate1", "minTideDate2", "maxTideDate2"};

		Map<String, Object> prevRow = tideInfoList.get(0);

		int prevTide = INVALID_VALUE;
		int prevTideX = INVALID_VALUE;
		String prevTideTime = null;

		for (int i = 0; i < tideValueKeys.length; i++) {
			Object tideObj = prevRow.get(tideValueKeys[i]);
			Object timeObj = prevRow.get(tideDateKeys[i]);

			if (tideObj == null || timeObj == null) {
				continue;
			}

			int tide = Integer.parseInt(tideObj.toString());
			String tideTime = timeObj.toString();

			if (prevTideTime == null || tideTime.compareTo(prevTideTime) > 0) {
				prevTide = tide;
				prevTideTime = tideTime;
				prevTideX = getTideIndex(baseDate, tideTime, sdf);
			}
		}

		Map<String, Object> nextRow = tideInfoList.get(tideInfoList.size() - 1);

		int nextTide = INVALID_VALUE;
		int nextTideX = INVALID_VALUE;
		String nextTideTime = null;

		for (int i = 0; i < tideValueKeys.length; i++) {
			Object tideObj = nextRow.get(tideValueKeys[i]);
			Object timeObj = nextRow.get(tideDateKeys[i]);

			if (tideObj == null || timeObj == null) {
				continue;
			}

			int tide = Integer.parseInt(tideObj.toString());
			String tideTime = timeObj.toString();

			if (nextTideTime == null || tideTime.compareTo(nextTideTime) < 0) {
				nextTide = tide;
				nextTideTime = tideTime;
				nextTideX = getTideIndex(baseDate, tideTime, sdf);
			}
		}

		List<Map<String, Object>> tidePointList = new ArrayList<Map<String, Object>>();

		for (int d = 1; d < tideInfoList.size() - 1; d++) {
			Map<String, Object> row = tideInfoList.get(d);

			for (int i = 1; i <= 2; i++) {
				Object maxObj = row.get("maxTide" + i);
				Object maxTimeObj = row.get("maxTideDate" + i);

				if (maxObj != null && maxTimeObj != null) {
					Map<String, Object> point = new HashMap<String, Object>();
					point.put("type", "MAX");
					point.put("value", Integer.parseInt(maxObj.toString()));
					point.put("time", maxTimeObj.toString());
					tidePointList.add(point);
				}

				Object minObj = row.get("minTide" + i);
				Object minTimeObj = row.get("minTideDate" + i);

				if (minObj != null && minTimeObj != null) {
					Map<String, Object> point = new HashMap<String, Object>();
					point.put("type", "MIN");
					point.put("value", Integer.parseInt(minObj.toString()));
					point.put("time", minTimeObj.toString());
					tidePointList.add(point);
				}
			}
		}

		Collections.sort(tidePointList, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				return o1.get("time").toString().compareTo(o2.get("time").toString());
			}
		});

		List<Map<String, Object>> debugTidePointList = new ArrayList<Map<String, Object>>();

		for (int i = 0; i < tidePointList.size(); i++) {
			Map<String, Object> point = tidePointList.get(i);
			String type = point.get("type").toString();
			int value = Integer.parseInt(point.get("value").toString());
			String time = point.get("time").toString();
			int tideX = getTideIndex(baseDate, time, sdf);

			Map<String, Object> debugPoint = new HashMap<String, Object>();
			debugPoint.put("seq", i);
			debugPoint.put("type", type);
			debugPoint.put("value", value);
			debugPoint.put("time", time);
			debugPoint.put("tideX", tideX);
			debugTidePointList.add(debugPoint);
		}

		int maxCnt = 0;
		int minCnt = 0;

		for (int i = 0; i < tidePointList.size(); i++) {
			Map<String, Object> point = tidePointList.get(i);

			if ("MAX".equals(point.get("type"))) {
				maxCnt++;
			} else {
				minCnt++;
			}
		}

		int[] maxTideList = new int[maxCnt];
		int[] maxTideXList = new int[maxCnt];
		int[] minTideList = new int[minCnt];
		int[] minTideXList = new int[minCnt];

		Arrays.fill(maxTideList, INVALID_VALUE);
		Arrays.fill(maxTideXList, INVALID_VALUE);
		Arrays.fill(minTideList, INVALID_VALUE);
		Arrays.fill(minTideXList, INVALID_VALUE);

		int maxIdx = 0;
		int minIdx = 0;

		for (int i = 0; i < tidePointList.size(); i++) {
			Map<String, Object> point = tidePointList.get(i);

			String type = point.get("type").toString();
			int value = Integer.parseInt(point.get("value").toString());
			String time = point.get("time").toString();

			int tideX = getTideIndex(baseDate, time, sdf);

			if ("MAX".equals(type)) {
				maxTideList[maxIdx] = value;
				maxTideXList[maxIdx] = tideX;
				maxIdx++;
			} else {
				minTideList[minIdx] = value;
				minTideXList[minIdx] = tideX;
				minIdx++;
			}
		}

		tideDrawInfo.put("maxTideList", maxTideList);
		tideDrawInfo.put("maxTideXList", maxTideXList);
		tideDrawInfo.put("minTideList", minTideList);
		tideDrawInfo.put("minTideXList", minTideXList);
		tideDrawInfo.put("prevTide", prevTide);
		tideDrawInfo.put("prevTideX", prevTideX);
		tideDrawInfo.put("nextTide", nextTide);
		tideDrawInfo.put("nextTideX", nextTideX);
		tideDrawInfo.put("debugTidePointList", debugTidePointList);

		return tideDrawInfo;
	}

	private int getTideIndex(Date baseDate, String tideTime, SimpleDateFormat sdf) throws Exception {
		
		Date tideDate = sdf.parse(tideTime);

		long diffMillis = tideDate.getTime() - baseDate.getTime();
		long oneDayMillis = 24L * 60L * 60L * 1000L;

		int dayOffset = (int) Math.floor(diffMillis / (double) oneDayMillis);
		int hour = Integer.parseInt(tideTime.substring(8, 10));
		int hourIndex = (hour - hour % 3) / 3;

		return dayOffset * 8 + hourIndex;
	}
	
    
	public void drawTide(Graphics2D g, Map<String, Object> tideDrawInfo, int fcstDaySize, int tideWidth, int tideHeight, int tideMarginLeft, int tideMarginTop) {
		
		if (g == null || tideDrawInfo == null) return;
		
		double tideCellWidth = tideWidth / (fcstDaySize * 8.0);
		
		int[] maxTideList = (int[]) tideDrawInfo.get("maxTideList");
		int[] maxTideXList = (int[]) tideDrawInfo.get("maxTideXList");
		int[] minTideList = (int[]) tideDrawInfo.get("minTideList");
		int[] minTideXList = (int[]) tideDrawInfo.get("minTideXList");
		
		Integer prevTideObj = (Integer) tideDrawInfo.get("prevTide");
		Integer prevTideXObj = (Integer) tideDrawInfo.get("prevTideX");
		Integer nextTideObj = (Integer) tideDrawInfo.get("nextTide");
		Integer nextTideXObj = (Integer) tideDrawInfo.get("nextTideX");
		
		if (maxTideList == null || maxTideXList == null || minTideList == null || minTideXList == null) return;
		if (prevTideObj == null || prevTideXObj == null || nextTideObj == null || nextTideXObj == null) return;
		if (maxTideList.length != maxTideXList.length || minTideList.length != minTideXList.length) return;
		
		int prevTide = prevTideObj.intValue();
		int prevTideX = prevTideXObj.intValue();
		int nextTide = nextTideObj.intValue();
		int nextTideX = nextTideXObj.intValue();
		
		int iconRadius = 18;
		int curveOffset = iconRadius + 4; // 곡선이 아이콘과 겹치는 것을 방지하기 위한 오프셋값
		int tideVerticalMargin = 8; // 위아래 여백 조절값
		
		int totalLength = maxTideList.length + minTideList.length;
		int[] values = new int[totalLength];
		int[] xsInput = new int[totalLength];
		boolean[] lowsInput = new boolean[totalLength];
		
		int p = 0;
		
		for (int i = 0; i < minTideList.length; i++) {
			values[p] = minTideList[i];
			xsInput[p] = minTideXList[i];
			lowsInput[p] = true;
			p++;
		}
		
		for (int i = 0; i < maxTideList.length; i++) {
			values[p] = maxTideList[i];
			xsInput[p] = maxTideXList[i];
			lowsInput[p] = false;
			p++;
		}
		
		int count = 0;
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != INVALID_VALUE && xsInput[i] != INVALID_VALUE) count++;
		}
		
		if (count < 1) return;
		
		int tideMin = Integer.MAX_VALUE;
		int tideMax = Integer.MIN_VALUE;
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != INVALID_VALUE && xsInput[i] != INVALID_VALUE) {
				tideMin = Math.min(tideMin, values[i]);
				tideMax = Math.max(tideMax, values[i]);
			}
		}
		
		if (prevTide != INVALID_VALUE && prevTideX != INVALID_VALUE) {
			tideMin = Math.min(tideMin, prevTide);
			tideMax = Math.max(tideMax, prevTide);
		}
		
		if (nextTide != INVALID_VALUE && nextTideX != INVALID_VALUE) {
			tideMin = Math.min(tideMin, nextTide);
			tideMax = Math.max(tideMax, nextTide);
		}
		
		if (tideMax == tideMin) tideMax = tideMin + 1;
		
		double drawableTop = tideMarginTop + tideVerticalMargin + iconRadius;
		double drawableBottom = tideMarginTop + tideHeight - tideVerticalMargin - iconRadius;
		double drawableHeight = drawableBottom - drawableTop;
		
		if (drawableHeight <= 0) return;
		
		double[] xs = new double[count];
		double[] ys = new double[count];
		boolean[] lows = new boolean[count];
		
		int idx = 0;
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != INVALID_VALUE && xsInput[i] != INVALID_VALUE) {
				double rate = (double) (values[i] - tideMin) / (double) (tideMax - tideMin);
				
				xs[idx] = tideMarginLeft + (xsInput[i] * tideCellWidth) + (tideCellWidth / 2.0);
				ys[idx] = drawableBottom - (drawableHeight * rate);
				lows[idx] = lowsInput[i];
				idx++;
			}
		}
		
		for (int i = 0; i < count - 1; i++) {
			for (int j = i + 1; j < count; j++) {
				if (xs[i] > xs[j]) {
					double tx = xs[i];
					xs[i] = xs[j];
					xs[j] = tx;
					
					double ty = ys[i];
					ys[i] = ys[j];
					ys[j] = ty;
					
					boolean tb = lows[i];
					lows[i] = lows[j];
					lows[j] = tb;
				}
			}
		}
		
		double[] curveXs = new double[count + 2];
		double[] curveYs = new double[count + 2];
		
		for (int i = 0; i < count; i++) {
			curveXs[i + 1] = xs[i];
			
			if (lows[i]) {
				curveYs[i + 1] = ys[i] + curveOffset;
			} else {
				curveYs[i + 1] = ys[i] - curveOffset;
			}
		}
		
		if (prevTide != INVALID_VALUE && prevTideX != INVALID_VALUE) {
			double prevRate = (double) (prevTide - tideMin) / (double) (tideMax - tideMin);
			curveXs[0] = tideMarginLeft + (prevTideX * tideCellWidth) + (tideCellWidth / 2.0);
			curveYs[0] = drawableBottom - (drawableHeight * prevRate);
		} else {
			curveXs[0] = tideMarginLeft;
			curveYs[0] = curveYs[1];
		}
		
		if (nextTide != INVALID_VALUE && nextTideX != INVALID_VALUE) {
			double nextRate = (double) (nextTide - tideMin) / (double) (tideMax - tideMin);
			curveXs[count + 1] = tideMarginLeft + (nextTideX * tideCellWidth) + (tideCellWidth / 2.0);
			curveYs[count + 1] = drawableBottom - (drawableHeight * nextRate);
		} else {
			curveXs[count + 1] = tideMarginLeft + tideWidth;
			curveYs[count + 1] = curveYs[count];
		}
		
		Path2D path = new Path2D.Double();
		path.moveTo(curveXs[0], curveYs[0]);
		
		for (int i = 0; i < curveXs.length - 1; i++) {
			double x1 = curveXs[i];
			double y1 = curveYs[i];
			double x2 = curveXs[i + 1];
			double y2 = curveYs[i + 1];
			double dx = (x2 - x1) * 0.45;
			
			path.curveTo(x1 + dx, y1, x2 - dx, y2, x2, y2);
		}
		
		Object oldAntialias = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
		Stroke oldStroke = g.getStroke();
		Color oldColor = g.getColor();
		Font oldFont = g.getFont();
		Shape oldClip = g.getClip();
		
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setStroke(new BasicStroke(2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
		g.setColor(new Color(0, 60, 130));
		g.setClip(tideMarginLeft, tideMarginTop, tideWidth, tideHeight);
		g.draw(path);
		g.setClip(oldClip);
		
		for (int i = 0; i < count; i++) {
			Ellipse2D circle = new Ellipse2D.Double(xs[i] - iconRadius, ys[i] - iconRadius, iconRadius * 2, iconRadius * 2);
			
			if (lows[i]) {
				g.setColor(new Color(40, 130, 220));
			 } else {
				g.setColor(new Color(220, 80, 60));
			}
			
			g.fill(circle);
			g.setColor(new Color(255, 255, 255));
			g.setStroke(new BasicStroke(2f));
			g.draw(circle);
			
			String text = lows[i] ? "저" : "고";
			
			g.setFont(new Font("Dialog", Font.BOLD, 18));
			
			FontMetrics fm = g.getFontMetrics();
			int tx = (int) (xs[i] - fm.stringWidth(text) / 2.0);
			int ty = (int) (ys[i] + fm.getAscent() / 2.0 - 2);
			
			g.drawString(text, tx, ty);
		}
		
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, oldAntialias);
		g.setStroke(oldStroke);
		g.setColor(oldColor);
		g.setFont(oldFont);
		g.setClip(oldClip);
	}
	
	private void printTideDrawInfo(Map<String, Object> tideDrawInfo) {
		
		List<Map<String, Object>> debugTidePointList = (List<Map<String, Object>>) tideDrawInfo.get("debugTidePointList");
		
		System.out.println("\n--- SORTED TIDE POINT LIST ---");

		if (debugTidePointList != null) {
			for (int i = 0; i < debugTidePointList.size(); i++) {
				Map<String, Object> point = debugTidePointList.get(i);

				System.out.println(
					"[" + point.get("seq") + "] " +
					"type=" + point.get("type") +
					", value=" + point.get("value") +
					", time=" + point.get("time") +
					", tideX=" + point.get("tideX")
				);
			}
		}
	}
	

	private int getCellTextLeftMargin(String text, int cellWidth, Font font) {

	    if (text == null || text.isEmpty()) {
	        return 0;
	    }

	    int fontSize = font.getSize();
	    int textWidth = 0;

	    for (char ch : text.toCharArray()) {

	        // 한글
	        if (Character.UnicodeBlock.of(ch) == Character.UnicodeBlock.HANGUL_SYLLABLES
	                || Character.UnicodeBlock.of(ch) == Character.UnicodeBlock.HANGUL_JAMO
	                || Character.UnicodeBlock.of(ch) == Character.UnicodeBlock.HANGUL_COMPATIBILITY_JAMO) {

	            textWidth += fontSize;

	        // 영어 / 숫자
	        } else if (Character.isLetterOrDigit(ch)) {

	            textWidth += (fontSize * 3) / 5;

	        // 공백
	        } else if (Character.isWhitespace(ch)) {

	            textWidth += fontSize / 2;

	        // 특수문자
	        } else {

	            textWidth += (fontSize * 2) / 3;
	        }
	    }

	    return Math.max((cellWidth - textWidth) / 2, 0);
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
		
		this.generateDfsTideTable("2026053100", 3);
		
		this.destroy(); // 자원 해제
    }

	public static void main(String[] args) {
		
		DfsTideFcstTableGenerator acim = new DfsTideFcstTableGenerator();
		acim.process(); // 프로세스 실행
		
    }

}
