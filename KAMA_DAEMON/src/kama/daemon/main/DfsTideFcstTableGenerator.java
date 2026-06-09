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
import java.io.RandomAccessFile;
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
import kama.daemon.common.util.KmaDfsConverter.DfsGrid;

public class DfsTideFcstTableGenerator {
	
	private final int GRID_WIDTH = 149;
	private final int GRID_HEIGHT = 253;
	
	private String[] regionInfoList = new String[] {"울산", "김해"};
	private String[] dfsPointInfoList = new String[] {"송정동|103,85", "대저2동|96,76"};
	private String[] tidePointInfoList = new String[] {"울산항|58", "다대포|115"};
	
	private final String DFS_PCP_SHRT_FILE_REGEX = "KMA_DFS_SHRT_PCP_{issuedTm}_{fcstTm}.bin";
	
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
			" AND STA_UID = {tideUid}                                          			"+
			" ORDER BY FCST_DATE ASC 													";
	
	
	private String getDfsProcInfoQuery = 
			
			" SELECT 								                             		"+
			" 	TO_CHAR(ISSUED_DT, 'YYYYMMDDHH24MI') AS ISSUED_DT, 				 		"+		
			" 	TO_CHAR(FCST_DT, 'YYYYMMDDHH24MI') AS FCST_DT, 	                 		"+
			" 	DFS_TYPE						                                 		"+
			" FROM AAMI.KMA_DFS_PROC_INFO 	                                     		"+
			" WHERE 1=1                                                          		"+
			" AND FCST_DT >= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')       	 		"+
			" AND FCST_DT <= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')+{fcstDaySize}   "+
			" AND DFS_TYPE = 'SHRT'                                              		"+
			" ORDER BY FCST_DT ASC, ISSUED_DT DESC                               		";
	
	
	private String getTideTimeInfoQuery = 
			
			" SELECT 								                             		"+		
			" 	TO_CHAR(FCST_DATE, 'YYYYMMDDHH24MI') AS FCST_DATE, 	                 	"+
			" 	TIDE_STATE						                                 		"+
			" FROM AAMI.KHOA_TIDE_TIME_INFO 	                                   		"+
			" WHERE 1=1                                                          		"+
			" AND FCST_DATE >= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')     			"+
			" AND FCST_DATE <= TO_DATE('{issuedTmStr}', 'YYYYMMDDHH24MI')+{fcstDaySize} "+
			" ORDER BY FCST_DATE ASC 				                               		";
	
	private String storePath = null;

	private static class TableLayout {
		int marginLeft;
		int marginTop;
		int marginRight;
		int marginBottom;

		int tableX;
		int tableY;
		int tableW;
		int tableH;

		int regionCount;
		int hourInterval;
		int cellsPerDay;

		int regionColW;
		int elementColW;
		int remarkColW;

		int dateHeaderH;
		int timeHeaderH;
		int headerH;

		int fcstAreaX;
		int fcstAreaRightX;
		int fcstAreaW;

		int bodyY;
		int regionRowH;
		int rainRowH;
		int tideRowH;
		int tideTypeRowH;

		double dayColW;
		double timeCellW;
	}

	private TableLayout createTableLayout(int fcstDaySize, int height) {

	    TableLayout layout = new TableLayout();

	    // ===== 전체 테이블 여백 =====
	    layout.marginLeft = 10;
	    layout.marginTop = 10;
	    layout.marginRight = 10;
	    layout.marginBottom = 10;

	    // ===== 기본 설정 =====
	    layout.regionCount = this.regionInfoList.length;
	    layout.hourInterval = 3;
	    layout.cellsPerDay = 24 / layout.hourInterval;

	    // ===== 셀 크기 =====
	    layout.timeCellW = 60;

	    // ===== 좌/우 고정 컬럼 =====
	    layout.regionColW = 50;
	    layout.elementColW = 100;
	    layout.remarkColW = 100;

	    // ===== 헤더 영역 =====
	    layout.dateHeaderH = 35;
	    layout.timeHeaderH = 35;
	    layout.headerH = layout.dateHeaderH + layout.timeHeaderH;

	    // ===== 예보 영역 =====
	    layout.fcstAreaW =
	        (int)(fcstDaySize * layout.cellsPerDay * layout.timeCellW);

	    // ===== 전체 width 계산 =====
	    layout.tableW =
	        layout.regionColW +
	        layout.elementColW +
	        layout.fcstAreaW +
	        layout.remarkColW;

	    layout.tableH =
	        height -
	        layout.marginTop -
	        layout.marginBottom;

	    // ===== 최종 이미지 크기 =====
	    layout.tableX = layout.marginLeft;
	    layout.tableY = layout.marginTop;

	    // 전체 이미지 width
	    layout.fcstAreaX =
	        layout.marginLeft +
	        layout.regionColW +
	        layout.elementColW;

	    layout.fcstAreaRightX =
	        layout.fcstAreaX +
	        layout.fcstAreaW;

	    // ===== 날짜 컬럼 =====
	    layout.dayColW =
	        layout.fcstAreaW / (double)fcstDaySize;

	    // ===== 본문 영역 =====
	    layout.bodyY =
	        layout.marginTop +
	        layout.headerH;

	    layout.regionRowH =
	        (layout.tableH - layout.headerH) / layout.regionCount;

	    // ===== 내부 row =====
	    layout.rainRowH =
	        (int)(layout.regionRowH / 3.5);

	    layout.tideRowH =
	        layout.regionRowH - layout.rainRowH;

	    layout.tideTypeRowH =
	        layout.tideRowH / 8;

	    return layout;
	}

	
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
			
			int height = 600;	    	
			
			TableLayout layout = createTableLayout(fcstDaySize, height);
			
			int width = 
				    layout.tableW +
				    layout.marginLeft +
				    layout.marginRight;
	    	
	        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
	        Graphics2D g = image.createGraphics();
	        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
	        g.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
	        
	        // 배경 설정
	        g.setColor(Color.WHITE);
	        g.fillRect(0, 0, width, height);
	     	
			Date issuedTm = sdf.parse(issuedTmStr);
	        
			System.out.println("\n::: Start Generate Dfs Tide Fcst :::");
			System.out.println("-> Issued Time: " + sdf2.format(issuedTm));
			System.out.println("-> Fcst Day Size: " + fcstDaySize);

	        this.createDfsTideTable(g, width, height, issuedTm, fcstDaySize); // 예보 테이블 정보 생성;
	        
	        g.dispose(); // Graphics2D 객체 자원 해제	        
            
	        String y = issuedTmStr.substring(0, 4);
        	String m = issuedTmStr.substring(4, 6);
        	String d = issuedTmStr.substring(6, 8);
        	String h = issuedTmStr.substring(8, 10);
        	
        	String imgFileDirPath = storePath+"/DFS_TIDE_FCST/"+y+"/"+m+"/"+d;
        	
        	//String imgFileDirPath = "C:/data/datastore/DFS_TIDE_FCST/"+y+"/"+m+"/"+d;
        	
            File imgFileDir = new File(imgFileDirPath);
            
            if(!imgFileDir.exists()) {
            	imgFileDir.mkdirs();
            }
            
            File imgFile = new File(imgFileDirPath + "/dfs_tide_fcst_f" + (String.format("%02d", fcstDaySize)) + "_"+issuedTmStr+".png");
            
            ImageIO.write(image, "png", imgFile);
            
            System.out.println("-> Create Dfs Tide Fcst Image: " + imgFile.getAbsolutePath());
            
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
		TableLayout layout = this.createTableLayout(fcstDaySize, height);

		// ===== 폰트 =====
		int headerFontSize = (int)(layout.dateHeaderH * 0.4);
		int dateFontSize = (int)(layout.dateHeaderH * 0.39);
		int timeFontSize = (int)(layout.dateHeaderH * 0.38);

		Font headerFont = this.getFont(headerFontSize, true);
		Font dateFont = this.getFont(dateFontSize, true);
		Font timeFont = this.getFont(timeFontSize, true);

		// ===== 헤더 배경 =====
		g.setColor(new Color(232, 232, 248));
		g.fillRect(layout.tableX, layout.tableY, layout.tableW, layout.headerH);

		g.setStroke(new BasicStroke(2));
		g.setColor(Color.BLACK);

		// ===== 좌측 헤더 =====
		g.drawRect(layout.marginLeft, layout.marginTop, layout.regionColW, layout.headerH);
		this.setCellText("구분", g, layout.marginLeft, layout.marginTop, layout.regionColW, layout.headerH, headerFont);

		g.drawRect(layout.marginLeft + layout.regionColW, layout.marginTop, layout.elementColW, layout.headerH);
		this.setCellText("기상요소", g, layout.marginLeft + layout.regionColW, layout.marginTop, layout.elementColW, layout.headerH, headerFont);

		// ===== 비고 헤더 =====
		g.drawRect(width - layout.marginRight - layout.remarkColW, layout.marginTop, layout.remarkColW, layout.headerH);

		// ===== 전체 헤더 테두리 =====
		g.drawRect(layout.tableX, layout.tableY, layout.tableW, layout.headerH);

		// ===== 지역별 좌측 컬럼 / 기상요소 컬럼 =====
		for (int regionIndex = 0; regionIndex < layout.regionCount; regionIndex++) {
			
			String region = regionInfoList[regionIndex];
			String dfsPointName = dfsPointInfoList[regionIndex].split("\\|")[0];
			String tidePointName = tidePointInfoList[regionIndex].split("\\|")[0];

			int rowY = layout.bodyY + regionIndex * layout.regionRowH;

			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(2));

			// 구분 컬럼
			g.drawRect(layout.marginLeft, rowY, layout.regionColW, layout.regionRowH);

			g.setColor(new Color(0, 0, 255));
			this.setCellText(region, g, layout.marginLeft, rowY, layout.regionColW, layout.regionRowH, headerFont);

			g.setColor(Color.BLACK);

			// 기상요소 텍스트
			this.setCellText("강수량", g, layout.marginLeft + layout.regionColW, rowY - headerFont.getSize() + headerFont.getSize() / 3, layout.elementColW, layout.rainRowH, headerFont);
			this.setCellText("(" + dfsPointName + ")", g, layout.marginLeft + layout.regionColW + headerFont.getSize() / 3, rowY + headerFont.getSize() / 2, layout.elementColW, layout.rainRowH, headerFont);

			this.setCellText("조위", g, layout.marginLeft + layout.regionColW, rowY + layout.rainRowH - headerFont.getSize(), layout.elementColW, layout.tideRowH, headerFont);
			this.setCellText("(" + tidePointName + ")", g, layout.marginLeft + layout.regionColW + headerFont.getSize() / 3, rowY + layout.rainRowH + headerFont.getSize(), layout.elementColW, layout.tideRowH, headerFont);

			// 기상요소 컬럼 테두리
			g.setStroke(new BasicStroke(2));
			g.drawRect(layout.marginLeft + layout.regionColW, rowY, layout.elementColW, layout.regionRowH);

			// 강수량 / 조위 구분선
			g.setStroke(new BasicStroke(1));
			g.drawLine(layout.marginLeft + layout.regionColW, rowY + layout.rainRowH, width - layout.marginRight, rowY + layout.rainRowH);

			// 조위 그래프 / 대조기·소조기 영역 구분선
			g.drawLine(
				layout.fcstAreaX,
				rowY + layout.rainRowH + layout.tideRowH - layout.tideTypeRowH,
				layout.fcstAreaRightX,
				rowY + layout.rainRowH + layout.tideRowH - layout.tideTypeRowH
			);
		}

		// ===== 지역 row 사이 굵은 구분선 =====
		g.setStroke(new BasicStroke(2));
		for (int regionIndex = 1; regionIndex < layout.regionCount; regionIndex++) {
			int y = layout.bodyY + regionIndex * layout.regionRowH;
			g.drawLine(layout.fcstAreaX, y, width - layout.marginRight, y);
		}

		// ===== 날짜 / 시간 헤더 =====
		cal.setTime(issuedTm);

		g.setStroke(new BasicStroke(1));
		g.setColor(Color.BLACK);

		for (int dayIndex = 0; dayIndex < fcstDaySize; dayIndex++) {
			
			int dayX = (int)Math.round(layout.fcstAreaX + dayIndex * layout.dayColW);
			int nextDayX = (int)Math.round(layout.fcstAreaX + (dayIndex + 1) * layout.dayColW);
			int dayW = nextDayX - dayX;

			String dayText = dateHeaderFormat.format(cal.getTime());

			this.setCellText(dayText, g, dayX, layout.marginTop, dayW, layout.dateHeaderH, dateFont);

			// 일 구분 세로선
			g.drawLine(dayX, layout.marginTop, dayX, layout.marginTop + layout.headerH);

			for (int timeIndex = 0; timeIndex < layout.cellsPerDay; timeIndex++) {
				
				int cellIndex = dayIndex * layout.cellsPerDay + timeIndex;

				int cellX = (int)Math.round(layout.fcstAreaX + cellIndex * layout.timeCellW);
				int nextCellX = (int)Math.round(layout.fcstAreaX + (cellIndex + 1) * layout.timeCellW);
				int cellW = nextCellX - cellX;

				String startHourText = hourFormat.format(cal.getTime());
				cal.add(Calendar.HOUR_OF_DAY, layout.hourInterval);
				String endHourText = hourFormat.format(cal.getTime());

				if ("00".equals(endHourText)) {
					endHourText = "24";
				}

				this.setCellText(
					startHourText + "~" + endHourText,
					g,
					cellX,
					layout.marginTop + layout.dateHeaderH,
					cellW,
					layout.timeHeaderH,
					timeFont
				);

				// 시간 헤더 세로선
				g.drawLine(cellX, layout.marginTop + layout.dateHeaderH, cellX, layout.marginTop + layout.headerH);

				// 본문 시간 세로선
				for (int regionIndex = 0; regionIndex < layout.regionCount; regionIndex++) {
					
					int rowY = layout.bodyY + regionIndex * layout.regionRowH;

					g.drawLine(cellX, rowY, cellX, rowY + layout.rainRowH);
					g.drawLine(cellX, rowY + layout.rainRowH, cellX, rowY + layout.regionRowH - layout.tideTypeRowH);
				}
			}
		}

		// 마지막 날짜 끝 세로선
		g.drawLine(layout.fcstAreaRightX, layout.marginTop, layout.fcstAreaRightX, layout.marginTop + layout.headerH);

		// 날짜 / 시간 가로 구분선
		g.drawLine(layout.fcstAreaX, layout.marginTop + layout.dateHeaderH, layout.fcstAreaRightX, layout.marginTop + layout.dateHeaderH);

		// 비고란 왼쪽 세로선
		g.drawLine(layout.fcstAreaRightX, layout.marginTop, layout.fcstAreaRightX, height - layout.marginBottom);

		// 전체 테두리
		g.setStroke(new BasicStroke(2));
		g.drawRect(layout.tableX, layout.tableY, layout.tableW, layout.tableH);

		g.setStroke(new BasicStroke(1));
		
		// ===== 동네예보 조회 =====
		
/*		for (int regionIndex = 0; regionIndex < dfsPointInfoList.length; regionIndex++) {

			String[] dfsPointInfo = dfsPointInfoList[regionIndex].split("\\|");
			
			int nx = Integer.parseInt(dfsPointInfo[1].split(",")[0]);
			int ny = Integer.parseInt(dfsPointInfo[1].split(",")[1]);

			Map<String, Object> dfsDrawInfo = getDfsDrawInfo(tideBaseFormat.format(issuedTm), fcstDaySize, nx, ny);

//			printDfsDrawInfo(dfsDrawInfo);
			
			drawDfs(
				g,
				dfsDrawInfo,
				fcstDaySize,
				layout.fcstAreaW,
				layout.rainRowH,
				layout.fcstAreaX,
				layout.bodyY + regionIndex * layout.regionRowH
			);
		}*/

		// ===== 조위 그래프 =====
		
		Map<String, Object> tideTimeDrawInfo = getTideTimeDrawInfo(tideBaseFormat.format(issuedTm), fcstDaySize);
		
		for (int regionIndex = 0; regionIndex < tidePointInfoList.length; regionIndex++) {
			
			String[] tidePointInfo = tidePointInfoList[regionIndex].split("\\|");
			int tidePointUid = Integer.parseInt(tidePointInfo[1]);

			Map<String, Object> tideDrawInfo = getTideDrawInfo(tideBaseFormat.format(issuedTm), fcstDaySize, tidePointUid);

			//printTideDrawInfo(tideDrawInfo);

			drawTide(
				g,
				tideDrawInfo,
				tideTimeDrawInfo,
				fcstDaySize,
				layout.fcstAreaW,
				layout.tideRowH - layout.tideTypeRowH,
				layout.tideTypeRowH,
				layout.fcstAreaX,
				layout.bodyY + regionIndex * layout.regionRowH + layout.rainRowH
			);
		}
	}

	public void drawDfs(
		Graphics2D g,
		Map<String, Object> dfsDrawInfo,
		int fcstDaySize,
		int dfsWidth,
		int dfsHeight,
		int dfsMarginLeft,
		int dfsMarginTop
	) {

		if (g == null || dfsDrawInfo == null) return;

		float[] minPcpList = (float[]) dfsDrawInfo.get("minPcpList");
		float[] maxPcpList = (float[]) dfsDrawInfo.get("maxPcpList");
		int[] pcpXList = (int[]) dfsDrawInfo.get("pcpXList");
		int[] pcpCountList = (int[]) dfsDrawInfo.get("pcpCountList");

		if (minPcpList == null || maxPcpList == null || pcpXList == null || pcpCountList == null) return;

		double dfsCellWidth = dfsWidth / (fcstDaySize * 8.0);

		Font oldFont = g.getFont();
		Color oldColor = g.getColor();

		Font pcpFont = this.getFont((int)(dfsHeight * 0.22), true);
		if (pcpFont == null) {
			pcpFont = new Font("Dialog", Font.BOLD, Math.max(10, (int)(dfsHeight * 0.22)));
		}

		g.setFont(pcpFont);
		g.setColor(Color.BLACK);

		for (int i = 0; i < pcpXList.length; i++) {

			if (pcpXList[i] == INVALID_VALUE || pcpCountList[i] == 0) {
				continue;
			}

			int pcpX = pcpXList[i];

			if (pcpX < 0 || pcpX >= fcstDaySize * 8) {
				continue;
			}

			int minValue = Math.round(minPcpList[i]);
			int maxValue = Math.round(maxPcpList[i]);
			
			// 둘 다 0이면 출력 안함
			if (minValue == 0 && maxValue == 0) {
				continue;
			}

			String text = minValue + " ~ " + maxValue;

			int cellX = (int)Math.round(dfsMarginLeft + pcpX * dfsCellWidth);
			int nextCellX = (int)Math.round(dfsMarginLeft + (pcpX + 1) * dfsCellWidth);
			int cellW = nextCellX - cellX;
			
			Color bgColor = null;

			if (minValue >= 20 || maxValue >= 20) {
			    bgColor = new Color(255, 230, 230); // 옅은 붉은색
			} else if (minValue >= 10 || maxValue >= 10) {
			    bgColor = new Color(230, 255, 230); // 옅은 초록색
			}

			if (bgColor != null) {
			    g.setColor(bgColor);
			    g.fillRect(cellX, dfsMarginTop, cellW, dfsHeight);

			    // 배경 때문에 사라진 셀 테두리 복구
			    g.setColor(Color.BLACK);
			    g.setStroke(new BasicStroke(1f));
			    g.drawRect(cellX, dfsMarginTop, cellW, dfsHeight);
			}

			this.setCellText(
				text,
				g,
				cellX,
				dfsMarginTop,
				cellW,
				dfsHeight,
				pcpFont
			);
			
			this.setCellText(
			    text,
			    g,
			    cellX + 1,
			    dfsMarginTop,
			    cellW,
			    dfsHeight,
			    pcpFont
			);
		}

		g.setFont(oldFont);
		g.setColor(oldColor);
	}
	
	private Map<String, Object> getDfsDrawInfo(String issuedTmStr, int fcstDaySize, int nx, int ny) throws Exception {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMddHH");

		System.out.println("-> Get DFS Draw Info [" + issuedTmStr + ", " + fcstDaySize + ", " + nx + ", " + ny + "]");

		String query = getDfsProcInfoQuery.replaceAll("\\{issuedTmStr\\}", issuedTmStr)
										  .replaceAll("\\{fcstDaySize\\}", fcstDaySize + "");

		List<Map<String, Object>> dfsProcInfoList = new ArrayList<Map<String, Object>>();

		ResultSet resultSet = dbManager.executeQuery(query);

		while (resultSet.next()) {
			Map<String, Object> dfsProcInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);
			dfsProcInfoList.add(dfsProcInfo);
		}

		if (dfsProcInfoList == null || dfsProcInfoList.size() == 0) {
			return null;
		}

		// FCST_DT 중복 제거: ORDER BY FCST_DT ASC, ISSUED_DT DESC 라서 첫 번째가 최신
		Map<String, String> dupMap = new HashMap<String, String>();
		for (int i = 0; i < dfsProcInfoList.size(); i++) {
			String fcstDt = dfsProcInfoList.get(i).get("fcstDt").toString();

			if (dupMap.containsKey(fcstDt)) {
				dfsProcInfoList.remove(i--);
			} else {
				dupMap.put(fcstDt, null);
			}
		}
		
		System.out.println("\t>> DFS Data Size: " + dfsProcInfoList.size());
		System.out.println("\t>> First DFS Fcst Tm: " + dfsProcInfoList.get(0).get("fcstDt"));
		System.out.println("\t>> Last DFS Fcst Tm: " + dfsProcInfoList.get(dfsProcInfoList.size()-1).get("fcstDt"));
		

		Date baseDate = sdf.parse(issuedTmStr);
		DfsGrid dfsGrid = new DfsGrid(nx, ny, 0, 0);

		int totalSlotCount = fcstDaySize * 8;

		float[] minPcpList = new float[totalSlotCount];
		float[] maxPcpList = new float[totalSlotCount];
		int[] pcpXList = new int[totalSlotCount];
		int[] pcpCountList = new int[totalSlotCount];

		Arrays.fill(minPcpList, Float.MAX_VALUE);
		Arrays.fill(maxPcpList, -Float.MAX_VALUE);
		Arrays.fill(pcpXList, INVALID_VALUE);
		Arrays.fill(pcpCountList, 0);

		List<Map<String, Object>> debugDfsPcpList = new ArrayList<Map<String, Object>>();

		for (int i = 0; i < dfsProcInfoList.size(); i++) {

			Date issuedDt = sdf.parse(dfsProcInfoList.get(i).get("issuedDt").toString());
			Date fcstDt = sdf.parse(dfsProcInfoList.get(i).get("fcstDt").toString());

			String dfsFilePath = storePath + "/KMA_DFS_BIN/" + sdf2.format(issuedDt);

			dfsFilePath = dfsFilePath + "/" + DFS_PCP_SHRT_FILE_REGEX
					.replaceAll("\\{issuedTm\\}", sdf3.format(issuedDt))
					.replaceAll("\\{fcstTm\\}", sdf3.format(fcstDt));
			
			File dfsFile = new File(dfsFilePath);
			if (!dfsFile.exists()) {
			    System.out.println("DFS file not found: " + dfsFilePath);
			    continue;
			}

			RandomAccessFile raf = null;

			try {
				
				raf = new RandomAccessFile(dfsFilePath, "r");

				float value = this.readDfsFile(raf, dfsGrid);

				int pcpX = getDfsIndex(baseDate, fcstDt);

				if (pcpX < 0 || pcpX >= totalSlotCount) {
					continue;
				}

				// 결측값 방어
				if (value == INVALID_VALUE || Float.isNaN(value)) {
					continue;
				}

				minPcpList[pcpX] = Math.min(minPcpList[pcpX], value);
				maxPcpList[pcpX] = Math.max(maxPcpList[pcpX], value);
				pcpXList[pcpX] = pcpX;
				pcpCountList[pcpX]++;

				Map<String, Object> debug = new HashMap<String, Object>();
				debug.put("issuedTm", sdf.format(issuedDt));
				debug.put("fcstTm", sdf.format(fcstDt));
				debug.put("value", value);
				debug.put("pcpX", pcpX);
				debugDfsPcpList.add(debug);

			} finally {
				if (raf != null) {
					raf.close();
				}
			}
		}

		for (int i = 0; i < totalSlotCount; i++) {
			if (pcpCountList[i] == 0) {
				minPcpList[i] = INVALID_VALUE;
				maxPcpList[i] = INVALID_VALUE;
			}
		}

		List<Map<String, Object>> debugPcpGroupList = new ArrayList<Map<String, Object>>();

		for (int i = 0; i < totalSlotCount; i++) {

			Map<String, Object> group = new HashMap<String, Object>();

			int startHour = (i % 8) * 3;
			int endHour = startHour + 3;

			group.put("pcpX", i);
			group.put("timeRange", String.format("%02d~%02d", startHour, endHour == 24 ? 24 : endHour));
			group.put("minPcp", minPcpList[i]);
			group.put("maxPcp", maxPcpList[i]);
			group.put("count", pcpCountList[i]);

			List<Map<String, Object>> hourList = new ArrayList<Map<String, Object>>();

			for (int j = 0; j < debugDfsPcpList.size(); j++) {
				Map<String, Object> item = debugDfsPcpList.get(j);

				if (Integer.parseInt(item.get("pcpX").toString()) == i) {
					hourList.add(item);
				}
			}

			group.put("hourList", hourList);
			debugPcpGroupList.add(group);
		}

		Map<String, Object> dfsDrawInfo = new HashMap<String, Object>();
		dfsDrawInfo.put("minPcpList", minPcpList);
		dfsDrawInfo.put("maxPcpList", maxPcpList);
		dfsDrawInfo.put("pcpXList", pcpXList);
		dfsDrawInfo.put("pcpCountList", pcpCountList);
		dfsDrawInfo.put("debugPcpGroupList", debugPcpGroupList);

		return dfsDrawInfo;
	}
	
	private Map<String, Object> getTideTimeDrawInfo(String issuedTmStr, int fcstDaySize) throws Exception {

	    System.out.println("-> Get Tide Time Draw Info [" + issuedTmStr + ", " + fcstDaySize + "]");

	    String query = getTideTimeInfoQuery.replaceAll("\\{issuedTmStr\\}", issuedTmStr)
	            .replaceAll("\\{fcstDaySize\\}", fcstDaySize + "");

	    List<Map<String, Object>> tideTimeInfoList = new ArrayList<Map<String, Object>>();

	    ResultSet resultSet = dbManager.executeQuery(query);

	    while (resultSet.next()) {
	        Map<String, Object> tideTimeInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);
	        tideTimeInfoList.add(tideTimeInfo);
	    }

	    Map<String, Object> tideTimeDrawInfo = new HashMap<String, Object>();
	    tideTimeDrawInfo.put("tideTimeInfoList", tideTimeInfoList);

	    return tideTimeDrawInfo;
	}
	
	private Map<String, Object> getTideDrawInfo(String issuedTmStr, int fcstDaySize, int tideUid) throws Exception {
		
		System.out.println("-> Get Tide Draw Info [" + issuedTmStr + ", " + fcstDaySize + ", " + tideUid + "]");
		
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
		Date baseDate = sdf.parse(issuedTmStr);

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
    
	public void drawTide(Graphics2D g, Map<String, Object> tideDrawInfo, Map<String, Object> tideTimeDrawInfo, int fcstDaySize, int tideWidth, int tideHeight, int tideTypeHeight, int tideMarginLeft, int tideMarginTop) {
		
		if (g == null || tideDrawInfo == null) return;
		
		List<Map<String, Object>> debugTidePointList = (List<Map<String, Object>>) tideDrawInfo.get("debugTidePointList");
		
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
		String[] timeInput = new String[totalLength];
		
		int p = 0;
		
		for (int i = 0; i < minTideList.length; i++) {
			values[p] = minTideList[i];
			xsInput[p] = minTideXList[i];
			lowsInput[p] = true;
			timeInput[p] = findTideTime(debugTidePointList, "MIN", minTideList[i], minTideXList[i]);
			p++;
		}
		
		for (int i = 0; i < maxTideList.length; i++) {
			values[p] = maxTideList[i];
			xsInput[p] = maxTideXList[i];
			lowsInput[p] = false;
			timeInput[p] = findTideTime(debugTidePointList, "MAX", maxTideList[i], maxTideXList[i]);
			p++;
		}
		
		int count = 0;
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != INVALID_VALUE && xsInput[i] != INVALID_VALUE) count++;
		}
		
		if (count < 1) {
			
			drawTideTypeRow(
		        g,
		        tideTimeDrawInfo,
		        fcstDaySize,
		        tideWidth,
		        tideTypeHeight,
		        tideMarginLeft,
		        tideMarginTop + tideHeight
		    );
		    return;
		}
		
		
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
		
		// ===== 실제 그래프 영역 높이 =====
		int tideGraphDrawHeight = (int)(tideHeight * 0.8);

		// ===== 그래프 영역 =====
		double drawableTop = tideMarginTop + tideVerticalMargin + iconRadius;

		double drawableBottom =
			tideMarginTop +
			tideGraphDrawHeight -
			tideVerticalMargin -
			iconRadius;

		double drawableHeight = drawableBottom - drawableTop;
		
		if (drawableHeight <= 0) return;
		
		double[] xs = new double[count];
		double[] ys = new double[count];
		boolean[] lows = new boolean[count];
		String[] times = new String[count];
		int[] drawValues = new int[count];
		
		int idx = 0;
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != INVALID_VALUE && xsInput[i] != INVALID_VALUE) {
				double rate = (double) (values[i] - tideMin) / (double) (tideMax - tideMin);
				
				xs[idx] = tideMarginLeft + (xsInput[i] * tideCellWidth) + (tideCellWidth / 2.0);
				ys[idx] = drawableBottom - (drawableHeight * rate);
				lows[idx] = lowsInput[i];
				times[idx] = timeInput[i];
				drawValues[idx] = values[i];
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
					
					String tt = times[i];
					times[i] = times[j];
					times[j] = tt;
					
					int tv = drawValues[i];
					drawValues[i] = drawValues[j];
					drawValues[j] = tv;
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
			double dx = (x2 - x1) * 0.35;
			
			path.curveTo(x1 + dx, y1, x2 - dx, y2, x2, y2);
		}
		
		Object oldAntialias = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
		Stroke oldStroke = g.getStroke();
		Color oldColor = g.getColor();
		Font oldFont = g.getFont();
		Shape oldClip = g.getClip();
		
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setStroke(new BasicStroke(2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
		
		g.setClip(tideMarginLeft, tideMarginTop, tideWidth, tideHeight);
		
		// 여기서 고조 배경 먼저 칠하기
		for (int i = 0; i < count; i++) {
			if (!lows[i]) {
				int tideCellIndex = (int)((xs[i] - tideMarginLeft) / tideCellWidth);
				int cellX = (int)Math.round(tideMarginLeft + tideCellIndex * tideCellWidth);
				int nextCellX = (int)Math.round(tideMarginLeft + (tideCellIndex + 1) * tideCellWidth);
				int cellW = nextCellX - cellX;

				g.setColor(new Color(255, 230, 230));
				g.fillRect(cellX, tideMarginTop, cellW, tideHeight);
			}
		}
		
		// ===== 배경 때문에 덮인 조위 칸 테두리 복구 =====
		g.setColor(Color.BLACK);
		g.setStroke(new BasicStroke(1f));

		// 세로선 복구
		for (int i = 0; i <= fcstDaySize * 8; i++) {
			int x = (int)Math.round(tideMarginLeft + i * tideCellWidth);
			g.drawLine(x, tideMarginTop, x, tideMarginTop + tideHeight);
		}

		// 상/하단 가로선 복구
		g.drawLine(tideMarginLeft, tideMarginTop, tideMarginLeft + tideWidth, tideMarginTop);
		g.drawLine(tideMarginLeft, tideMarginTop + tideHeight, tideMarginLeft + tideWidth, tideMarginTop + tideHeight);

		g.setColor(new Color(0, 60, 130));
		g.setStroke(new BasicStroke(2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
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
			
			// ===== 조위 값 / 시간 표시 =====

			String tideValueText = drawValues[i] + "cm";

			String tideTimeText = "";

			if (times[i] != null && times[i].length() >= 12) {
				tideTimeText = "(" + times[i].substring(8, 10) + ":" + times[i].substring(10, 12) + ")";
			}
			
			Font infoFont = new Font("Dialog", Font.BOLD, 14);
			g.setFont(infoFont);

			FontMetrics infoFm = g.getFontMetrics();

			// 첫 줄 : 13cm
			int valueTx = (int)(xs[i] - infoFm.stringWidth(tideValueText) / 2.0);
			int valueTy = (int)(ys[i] + iconRadius + 21);

			g.setColor(lows[i] ? new Color(40, 130, 220) : new Color(220, 80, 60));
			g.drawString(tideValueText, valueTx, valueTy);

			// 둘째 줄 : (02:21)
			int timeTx = (int)(xs[i] - infoFm.stringWidth(tideTimeText) / 2.0);
			int timeTy = valueTy + 14;

			g.drawString(tideTimeText, timeTx, timeTy);
		}
		
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, oldAntialias);
		g.setStroke(oldStroke);
		g.setColor(oldColor);
		g.setFont(oldFont);
		g.setClip(oldClip);
		
		drawTideTypeRow(
		    g,
		    tideTimeDrawInfo,
		    fcstDaySize,
		    tideWidth,
		    tideTypeHeight,
		    tideMarginLeft,
		    tideMarginTop + tideHeight
		);
	}
	
	private void drawTideTypeRow(
	    Graphics2D g,
	    Map<String, Object> tideTimeDrawInfo,
	    int fcstDaySize,
	    int tideWidth,
	    int tideTypeHeight,
	    int tideMarginLeft,
	    int tideTypeY
	) {

	    if (g == null || tideTimeDrawInfo == null) {
	        return;
	    }

	    List<Map<String, Object>> tideTimeInfoList =
	        (List<Map<String, Object>>) tideTimeDrawInfo.get("tideTimeInfoList");

	    if (tideTimeInfoList == null || tideTimeInfoList.size() == 0) {
	        return;
	    }

	    double dayWidth = tideWidth / (double) fcstDaySize;

	    Font oldFont = g.getFont();
	    Color oldColor = g.getColor();
	    Stroke oldStroke = g.getStroke();

	    Font tideTypeFont = this.getFont((int)(tideTypeHeight * 0.55), true);
	    if (tideTypeFont == null) {
	        tideTypeFont = new Font("Dialog", Font.BOLD, Math.max(10, (int)(tideTypeHeight * 0.55)));
	    }

	    int startDayIndex = 0;
	    String prevState = null;

	    for (int i = 0; i < fcstDaySize; i++) {

	        String state = null;

	        if (i < tideTimeInfoList.size()) {
	            Object stateObj = tideTimeInfoList.get(i).get("tideState");
	            if (stateObj != null) {
	                state = stateObj.toString();
	            }
	        }

	        if (state == null || state.trim().length() == 0) {
	            state = "";
	        }

	        if (i == 0) {
	            prevState = state;
	            startDayIndex = 0;
	            continue;
	        }

	        if (!state.equals(prevState)) {
	            drawTideTypeCell(
	                g,
	                prevState,
	                startDayIndex,
	                i,
	                dayWidth,
	                tideTypeHeight,
	                tideMarginLeft,
	                tideTypeY,
	                tideTypeFont
	            );

	            startDayIndex = i;
	            prevState = state;
	        }
	    }

	    drawTideTypeCell(
	        g,
	        prevState,
	        startDayIndex,
	        fcstDaySize,
	        dayWidth,
	        tideTypeHeight,
	        tideMarginLeft,
	        tideTypeY,
	        tideTypeFont
	    );

	    g.setColor(Color.BLACK);
	    g.setStroke(new BasicStroke(1f));

	    // 전체 테두리
	    g.drawRect(tideMarginLeft, tideTypeY, tideWidth, tideTypeHeight);


	    g.setFont(oldFont);
	    g.setColor(oldColor);
	    g.setStroke(oldStroke);
	}
	
	private void drawTideTypeCell(
	    Graphics2D g,
	    String tideState,
	    int startDayIndex,
	    int endDayIndex,
	    double dayWidth,
	    int tideTypeHeight,
	    int tideMarginLeft,
	    int tideTypeY,
	    Font font
	) {

	    if (tideState == null || tideState.length() == 0) {
	        return;
	    }

	    int cellX = (int)Math.round(tideMarginLeft + startDayIndex * dayWidth);
	    int nextCellX = (int)Math.round(tideMarginLeft + endDayIndex * dayWidth);
	    int cellW = nextCellX - cellX;

	    if ("대조기".equals(tideState)) {
	        g.setColor(new Color(155, 229, 255));
	    } else if ("소조기".equals(tideState)) {
	        g.setColor(new Color(255, 235, 200));
	    } else {
	        g.setColor(Color.WHITE);
	    }

	    g.fillRect(cellX, tideTypeY, cellW, tideTypeHeight);

	    g.setColor(Color.BLACK);
	    g.setStroke(new BasicStroke(1f));
	    g.drawRect(cellX, tideTypeY, cellW, tideTypeHeight);
	    
	    g.setStroke(new BasicStroke(2f));
	    g.drawLine(cellX, tideTypeY + tideTypeHeight, nextCellX, tideTypeY + tideTypeHeight);

	    this.setCellText(
	        tideState,
	        g,
	        cellX,
	        tideTypeY,
	        cellW,
	        tideTypeHeight,
	        font
	    );
	}
	
	private String findTideTime(
		List<Map<String, Object>> debugTidePointList,
		String type,
		int value,
		int tideX
	) {

		if (debugTidePointList == null) {
			return null;
		}

		for (int i = 0; i < debugTidePointList.size(); i++) {
			Map<String, Object> point = debugTidePointList.get(i);

			if (!type.equals(point.get("type"))) {
				continue;
			}

			int pointValue = Integer.parseInt(point.get("value").toString());
			int pointX = Integer.parseInt(point.get("tideX").toString());

			if (pointValue == value && pointX == tideX) {
				return point.get("time").toString();
			}
		}

		return null;
	}
	
	private void printDfsDrawInfo(Map<String, Object> dfsDrawInfo) {

		if (dfsDrawInfo == null) {
			System.out.println("DFS Draw Info is null");
			return;
		}

		List<Map<String, Object>> debugPcpGroupList =
			(List<Map<String, Object>>) dfsDrawInfo.get("debugPcpGroupList");

		System.out.println("\n--- DFS PCP GROUP LIST ---");

		if (debugPcpGroupList == null) {
			return;
		}

		for (int i = 0; i < debugPcpGroupList.size(); i++) {

			Map<String, Object> group = debugPcpGroupList.get(i);

			System.out.println(
				"[" + group.get("pcpX") + "] " +
				group.get("timeRange") +
				", minPcp=" + group.get("minPcp") +
				", maxPcp=" + group.get("maxPcp") +
				", count=" + group.get("count")
			);

			List<Map<String, Object>> hourList =
				(List<Map<String, Object>>) group.get("hourList");

			if (hourList != null) {
				for (int j = 0; j < hourList.size(); j++) {
					Map<String, Object> item = hourList.get(j);

					System.out.println(
						"    - fcstTm=" + item.get("fcstTm") +
						", issuedTm=" + item.get("issuedTm") +
						", value=" + item.get("value")
					);
				}
			}
		}
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
	
	private int getDfsIndex(Date baseDate, Date fcstDate) throws Exception {

		long diffMillis = fcstDate.getTime() - baseDate.getTime();
		long oneDayMillis = 24L * 60L * 60L * 1000L;

		int dayOffset = (int)Math.floor(diffMillis / (double)oneDayMillis);

		Calendar cal = new GregorianCalendar();
		cal.setTime(fcstDate);

		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int hourIndex = (hour - hour % 3) / 3;

		return dayOffset * 8 + hourIndex;
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
    

 	
 	private float readDfsFile(RandomAccessFile raf, DfsGrid dfsGrid) throws Exception {
 				
 		// 각 값은 4바이트이므로, (y * 가로갯수 + x) * 4 위치로 이동

 		long position = (dfsGrid.ny * GRID_WIDTH + dfsGrid.nx) * 4L;
 		raf.seek(position);

 		// 4바이트 읽기
 		byte[] bytes = new byte[4];
 		raf.readFully(bytes);

 		// 바이트 배열을 float로 변환
 		java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
 		bb.order(java.nio.ByteOrder.BIG_ENDIAN); // 빅엔디안으로 설정
 		return bb.getFloat();
 	}
    
    public void process() {
    	
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    	
    	System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
    	
		if(!this.initialize()) {
			
			System.out.println("Error : DfsTideFcstTableGenerator.process -> initialize failed");
			return;
		}
		
		// 현재 실행일 기준으로 +1~+3 일치까지 생성함
		Calendar cal = new GregorianCalendar();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		
		for(int i=1 ; i<=3 ; i++) {		
			this.generateDfsTideTable(sdf.format(cal.getTime()), i);
		}
		
		this.destroy(); // 자원 해제
    }

	public static void main(String[] args) {
		
		DfsTideFcstTableGenerator acim = new DfsTideFcstTableGenerator();
		acim.process(); // 프로세스 실행
		
    }

}
