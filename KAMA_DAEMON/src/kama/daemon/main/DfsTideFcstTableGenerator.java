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

public class DfsTideFcstTableGenerator {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
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
			
			System.out.println("Error : DfsTideFcstTableGenerator.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}
	
	private boolean generateFcstTable(String issuedTmStr) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("MM-dd");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH");
		
		try {
			
			int width = 1276;
			int height = 810;	    	
	    	
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
	        
	        Map<String, Object> dfsTideFcstInfo = this.getDfsTideFcstInfo(issuedTm);

	        this.createTableInfo(g, width, height, issuedTm, dfsTideFcstInfo); // 예보 테이블 정보 생성;
	        
			System.out.println("\n::: Start Generate Sector Table :::");
					
			System.out.println("-> Issued Time: " + sdf3.format(issuedTm));
	        
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
	
	private void createTableInfo(Graphics2D g, int width, int height, Date issuedTm, Map<String, Object> dfsTideFcstInfo) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("MM월 dd일 (E)");
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMddHH");
		
		Calendar cal = new GregorianCalendar();
		
		int tableLeftMargin = 40; // 왼쪽 여백
		int tableTopMargin = 200; // 위쪽 여백
		int tableRightMargin = 40; // 오른쪽 여백
		int tableBottomMargin = 80; // 아래쪽 여백

		int tableWidth = width - tableLeftMargin - tableRightMargin;
		int tableHeight = height - tableTopMargin - tableBottomMargin;

		int fcstDaySize = 2;
		int fcstPointSize = 2;
		int fcstHourInterval = 3;

		int kindColWidth = 50; // 첫 번째 열 (지점 이름)
		int elementColWidth = 100; // 두 번째 열 (기상요소)
		int etcColWidth = 100; // 세 번째 열 (섹터 이름)

		int dayRowHeight = 35; // 첫 번째 행 (날짜)
		int timeRowHeight = 35; // 두 번째 행 (시간)

		int fcstColWidth = (tableWidth - kindColWidth - elementColWidth - etcColWidth) / fcstDaySize;
		int fcstRowHeight = (tableHeight - dayRowHeight - timeRowHeight) / fcstPointSize;	
		
		int fcstDfsHeight = (int)(fcstRowHeight / 3.5);
		int fcstTideHeight = fcstRowHeight - fcstDfsHeight;
		int fcstTidalHeight = fcstTideHeight / 10;
		
		int fcstCellWidth = fcstColWidth / (24 / fcstHourInterval); // 예보 시간 간격에 따른 셀 너비 계산
        
    	int fontSize1 = (int) (dayRowHeight * 0.4); // 셀 높이에 비례하여 폰트 크기 조정
    	int fontSize2 = (int) (dayRowHeight * 0.37); // 셀 높이에 비례하여 폰트 크기 조정
    	int fontSize3 = (int) (dayRowHeight * 0.35); // 셀 높이에 비례하여 폰트 크기 조정
        
    	Font font1 = this.getFont(fontSize1, true);
    	Font font2 = this.getFont(fontSize2, true);
    	Font font3 = this.getFont(fontSize3, true);    
    	
        g.setColor(new Color(232,232,248)); // 연한 회색 배경;
        g.fillRect(tableLeftMargin, tableTopMargin, tableWidth, dayRowHeight + timeRowHeight); // 구분 열 배경
        
        g.setStroke(new BasicStroke(2)); // 선 굵기 5px
        g.setColor(Color.BLACK);
        
		// 구분 테두리 그리기
		g.drawRect(tableLeftMargin, tableTopMargin, kindColWidth, dayRowHeight + timeRowHeight);  
		
		this.setCellText("구분", g, tableLeftMargin, tableTopMargin, kindColWidth, dayRowHeight + timeRowHeight, font1);

		// 기상요소 테두리 그리기
        g.drawRect(tableLeftMargin + kindColWidth, tableTopMargin, elementColWidth, dayRowHeight + timeRowHeight);
        
        this.setCellText("기상요소", g, tableLeftMargin + kindColWidth, tableTopMargin, elementColWidth, dayRowHeight + timeRowHeight, font1);
        
        // 비고 테두리 그리기
        g.drawRect(width - tableRightMargin - etcColWidth, tableTopMargin, etcColWidth, dayRowHeight + timeRowHeight);

        // 헤더 테두리 그리기
        g.drawRect(tableLeftMargin, tableTopMargin, tableWidth, dayRowHeight + timeRowHeight);  
           
	    
        String[] regionInfo = (String[])dfsTideFcstInfo.get("regionInfo");
        List<String> dfsPointNameList = (List<String>)dfsTideFcstInfo.get("dfsPointNameList");
        List<String> tidePointNameList = (List<String>)dfsTideFcstInfo.get("tidePointNameList");
        
        List<Map<String, Object>> dfsFcstList = (List<Map<String, Object>>)dfsTideFcstInfo.get("dfsFcstList");
        List<Map<String, Object>> tideFcstList = (List<Map<String, Object>>)dfsTideFcstInfo.get("tideFcstList");
    	
		for (int i = 0; i < regionInfo.length; i++) {

			int y = tableTopMargin + dayRowHeight + timeRowHeight + i * fcstRowHeight;

			// 좀 굵은선으로 박스치기
			g.setColor(new Color(0,0,0));
			g.setStroke(new BasicStroke(2)); // 선 굵기 5px
			g.drawRect(tableLeftMargin, y, kindColWidth, fcstRowHeight); // 구분 열 테두리
			
			g.setColor(new Color(0,0,255)); // 연한 회색 배경;
			this.setCellText(regionInfo[i], g, tableLeftMargin, y, kindColWidth, fcstRowHeight, font1);
			
			String dfsPointName = dfsPointNameList.get(i);
			
			g.setColor(new Color(0,0,0));
			
			// 기상요소 안에 텍스트 넣기
			this.setCellText("강수량", g, tableLeftMargin + kindColWidth, y - font1.getSize(), elementColWidth, fcstDfsHeight, font1);
			this.setCellText("("+dfsPointName+")", g, tableLeftMargin + kindColWidth + font1.getSize()/3, y + font1.getSize(), elementColWidth, fcstDfsHeight, font1);
			
			// 조위 박스치기			
			g.setColor(new Color(0,0,0));
			g.setStroke(new BasicStroke(2)); // 선 굵기 5px
			
			// 조위안에 텍스트 넣기
			String tidePointName = tidePointNameList.get(i);
			this.setCellText("조위", g, tableLeftMargin + kindColWidth, y + fcstDfsHeight - font1.getSize(), elementColWidth, fcstTideHeight, font1);
			this.setCellText("("+tidePointName+")", g, tableLeftMargin + kindColWidth + font1.getSize()/3, y + fcstDfsHeight + font1.getSize(), elementColWidth, fcstTideHeight, font1);
			
			// 기상요소 박스치키
			g.setColor(new Color(0,0,0));
			g.setStroke(new BasicStroke(2)); // 선 굵기 5px
			g.drawRect(tableLeftMargin + kindColWidth, y, elementColWidth, fcstDfsHeight + fcstTideHeight); // 기상요소 열 테두리
			
			// 강수량칸 밑에 라인하나 그리기, 얇은선으로 끝까지
			g.setStroke(new BasicStroke(1)); // 선 굵기 5px
			g.drawLine(tableLeftMargin + kindColWidth, y + fcstDfsHeight, width - tableRightMargin, y + fcstDfsHeight); // 구분선
		}      
        // 셀 테두리 그리기 
		
		// 가운데선 끝까지 그리기
		g.setStroke(new BasicStroke(2));
		g.drawLine(tableLeftMargin + kindColWidth + elementColWidth, tableTopMargin + dayRowHeight + timeRowHeight + fcstRowHeight, width - tableRightMargin, tableTopMargin + dayRowHeight + timeRowHeight + fcstRowHeight); // 구분선
		
		
		
		cal.setTime(issuedTm);
        
		g.setStroke(new BasicStroke(1)); // 선 굵기 5px
        // 시간 구분선 그리기
        for (int i=0 ; i<fcstDaySize ; i++) {
        	
        	String dayText = sdf.format(cal.getTime());
        	
        	this.setCellText(dayText, g, tableLeftMargin + kindColWidth + elementColWidth + i * fcstColWidth, tableTopMargin, fcstColWidth, dayRowHeight, font2);
        	
			for (int j = 0; j < 24 / fcstHourInterval; j++) {
				
				String timeText = sdf2.format(cal.getTime());
				
				cal.add(Calendar.HOUR_OF_DAY, fcstHourInterval);
				
				String nextTimeText = sdf2.format(cal.getTime());
				
				if (nextTimeText.equals("00")) {
					nextTimeText = "24";
				}
				
				this.setCellText(timeText + "~" + nextTimeText, g, tableLeftMargin + kindColWidth + elementColWidth + i * fcstColWidth + j * fcstCellWidth, tableTopMargin + dayRowHeight, fcstCellWidth, timeRowHeight, font3);
				
				int x = tableLeftMargin + kindColWidth + elementColWidth + i * fcstColWidth + j * fcstCellWidth;
				g.drawLine(x, tableTopMargin + dayRowHeight, x, tableTopMargin + dayRowHeight + timeRowHeight); // 구분선
			}
        }
        
        // 일, 시간 구분선 그리기
        g.setStroke(new BasicStroke(1)); // 선 굵기 5px
        g.drawLine(tableLeftMargin + kindColWidth + elementColWidth, tableTopMargin + dayRowHeight, width - tableRightMargin - etcColWidth, tableTopMargin + dayRowHeight); // 구분선
        
        // 일 가운데 구분선 수직으로 그리기
        g.drawLine(tableLeftMargin + kindColWidth + elementColWidth + fcstColWidth, tableTopMargin, tableLeftMargin + kindColWidth + elementColWidth + fcstColWidth, tableTopMargin + dayRowHeight + timeRowHeight); // 구분선
				
		g.setStroke(new BasicStroke(2)); // 선 굵기 5px
        g.drawRect(tableLeftMargin, tableTopMargin, width - tableLeftMargin - tableRightMargin, height - tableTopMargin - tableBottomMargin);
        
        g.setStroke(new BasicStroke(1)); // 선 굵기 5px
        
        for (int i=0 ; i<dfsPointNameList.size() ; i++) {
        	
        	// 조위칸에 대조기/소조기 구분선 끝까지 그리기
        	
        	g.drawLine(tableLeftMargin + kindColWidth + elementColWidth, tableTopMargin + dayRowHeight + timeRowHeight + fcstDfsHeight + i*fcstRowHeight + (fcstTideHeight - fcstTidalHeight), 
        			width - tableRightMargin - etcColWidth, tableTopMargin + dayRowHeight + timeRowHeight + fcstDfsHeight + i*fcstRowHeight + (fcstTideHeight - fcstTidalHeight)); // 강수량 구분선
        	
            for (int j=0 ; j<fcstDaySize ; j++) {        	
             	
    			for (int k = 0; k < 24 / fcstHourInterval; k++) {
    				
    				String startTmStr = sdf3.format(cal.getTime());
    				
    				cal.add(Calendar.HOUR_OF_DAY, fcstHourInterval);
    				
    				String endTmStr = sdf3.format(cal.getTime());
    				
    				// 일단 강수량과 조위 부분에 라인을 그리자. 강수량칸 까지만 일단 그려야해
    				
    				int x = tableLeftMargin + kindColWidth + elementColWidth + j * fcstColWidth + k * fcstCellWidth;
    				int y = tableTopMargin + dayRowHeight + timeRowHeight + i * fcstRowHeight;

    				// ------- 이부분에서 동네예보 셋팅하기 ---------
    				
    				
    				// ------- 이부분에서 동네예보 셋팅하기 ---------
    				
    				g.drawLine(x, y, x, y + fcstDfsHeight); // 강수량 구분선
    				
    				g.drawLine(x, y + fcstDfsHeight, x, y + fcstRowHeight - fcstTidalHeight); // 조위 구분선;
    			}
            }
            

			
			g.drawLine(width - tableRightMargin - etcColWidth, tableTopMargin + dayRowHeight + timeRowHeight + i*fcstRowHeight, 
					width - tableRightMargin - etcColWidth, tableTopMargin + dayRowHeight + timeRowHeight + (i+1)*fcstRowHeight); // 강수량 구분선
        }
        
        // 조위 그래프 테스트
        drawTide(
            g,

            new int[] { 720, 680, 720, 680},

            new int[] {
            		fcstCellWidth*2 + fcstCellWidth/2, 
            		fcstCellWidth*6 + fcstCellWidth/2,
            		fcstCellWidth*10 + fcstCellWidth/2, 
            		fcstCellWidth*14 + fcstCellWidth/2
            },
            new int[] { 320, 280, 320, 280},

            new int[] { 
            		fcstCellWidth*0 + fcstCellWidth/2, 
            		fcstCellWidth*4 + fcstCellWidth/2,
            		fcstCellWidth*8 + fcstCellWidth/2, 
            		fcstCellWidth*12 + fcstCellWidth/2
            },

            // tideWidth, tideHeight
            fcstCellWidth*16, (int)(fcstTideHeight * 0.8),
            
            // tideLeftMargin, tideTopMargin
            tableLeftMargin + kindColWidth + elementColWidth,
            tableTopMargin + dayRowHeight + timeRowHeight + fcstDfsHeight 
        );
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
        
        String titleText = "예측 정보";
        
        g.drawString(titleText, this.getCellTextLeftMargin(titleText, width, titleFont)+50, topMargin + titleFont.getSize() + 10); // 수직 중앙 정렬
        
        g.setFont(infoFont);
        g.drawString("생산시각: " + sdf.format(issuedTm) + "UTC", leftMargin + logoSize + 15, topMargin + titleHeight + infoFont.getSize() + 15); // 범례 텍스트 위치 조정);
        
        String agency = "항공기상청 예보과";
        g.drawString(agency, width - agency.length()*infoFont.getSize(), topMargin + titleHeight + infoFont.getSize() + 15); // 범례 텍스트 위치 조정);
		
	}
	
	private Map<String, Object> getDfsTideFcstInfo(Date issuedTm) {
		
		Map<String, Object> resultMap = new HashMap<>();
		
		String[] regionInfo = new String[] {"울산", "김해"};
		String[] dfsPointInfo = new String[] {"송정동|0,0", "대저2동|0,0"};
		String[] tidePointInfo = new String[] {"울산항|0", "다대포|0"};
		
		List<String> dfsPointNameList = new ArrayList<>();
		List<String> tidePointNameList = new ArrayList<>();
		
		for (String dfsPoint : dfsPointInfo) {
			String[] parts = dfsPoint.split("\\|");
			dfsPointNameList.add(parts[0]);
		}
		
		for (String tidePoint : tidePointInfo) {
			String[] parts = tidePoint.split("\\|");
			tidePointNameList.add(parts[0]);
		}
		
		resultMap.put("regionInfo", regionInfo);
		resultMap.put("dfsPointNameList", dfsPointNameList);
		resultMap.put("tidePointNameList", tidePointNameList);
		
		return resultMap;
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
    
    public void drawTide(
            java.awt.Graphics2D g,
            int[] maxTideList,
            int[] maxTideXList,
            int[] minTideList,
            int[] minTideXList,
            int tideWidth, int tideHeight,
            int tideMarginLeft,
            int tideMarginTop
    ) {
        if (g == null ||
                maxTideList == null || maxTideXList == null ||
                minTideList == null || minTideXList == null) {
            return;
        }

        if (maxTideList.length != maxTideXList.length ||
                minTideList.length != minTideXList.length ||
                maxTideList.length != minTideList.length) {
            return;
        }

        int iconRadius = 18;
        int curveOffset = iconRadius + 4;

        int yPaddingTop = iconRadius + 10;
        int yPaddingBottom = iconRadius + 10;

        int totalLength = maxTideList.length + minTideList.length;

        int[] values = new int[totalLength];
        int[] xsInput = new int[totalLength];
        boolean[] isLowInput = new boolean[totalLength];
        int[] pairIndexInput = new int[totalLength];

        int p = 0;

        for (int i = 0; i < minTideList.length; i++) {
            values[p] = minTideList[i];
            xsInput[p] = minTideXList[i];
            isLowInput[p] = true;
            pairIndexInput[p] = i;
            p++;
        }

        for (int i = 0; i < maxTideList.length; i++) {
            values[p] = maxTideList[i];
            xsInput[p] = maxTideXList[i];
            isLowInput[p] = false;
            pairIndexInput[p] = i;
            p++;
        }

        int count = 0;

        for (int i = 0; i < values.length; i++) {
            if (values[i] >= 0 && xsInput[i] >= 0) {
                count++;
            }
        }

        if (count < 2) {
            return;
        }

        int tideMin = Integer.MAX_VALUE;
        int tideMax = Integer.MIN_VALUE;

        for (int i = 0; i < values.length; i++) {
            if (values[i] >= 0 && xsInput[i] >= 0) {
                tideMin = Math.min(tideMin, values[i]);
                tideMax = Math.max(tideMax, values[i]);
            }
        }

        if (tideMax == tideMin) {
            tideMax = tideMin + 1;
        }

        double drawableTop = tideMarginTop + yPaddingTop;
        double drawableBottom = tideMarginTop + tideHeight - yPaddingBottom;
        double drawableHeight = drawableBottom - drawableTop;

        double[] xs = new double[count];
        double[] ys = new double[count];
        boolean[] lows = new boolean[count];
        int[] pairIndexes = new int[count];

        int idx = 0;

        for (int i = 0; i < values.length; i++) {
            if (values[i] >= 0 && xsInput[i] >= 0) {
                double rate =
                        (double) (values[i] - tideMin) /
                                (double) (tideMax - tideMin);

                xs[idx] = tideMarginLeft + xsInput[i];
                ys[idx] = drawableBottom - (drawableHeight * rate);
                lows[idx] = isLowInput[i];
                pairIndexes[idx] = pairIndexInput[i];

                idx++;
            }
        }

        // x좌표 기준 정렬
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

                    int tp = pairIndexes[i];
                    pairIndexes[i] = pairIndexes[j];
                    pairIndexes[j] = tp;
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

        // 왼쪽 가상점
        int firstPairIndex = pairIndexes[0];

        int leftPairValue;
        int leftPairX;
        boolean leftPairIsLow;

        if (lows[0]) {
            leftPairValue = maxTideList[firstPairIndex];
            leftPairX = maxTideXList[firstPairIndex];
            leftPairIsLow = false;
        } else {
            leftPairValue = minTideList[firstPairIndex];
            leftPairX = minTideXList[firstPairIndex];
            leftPairIsLow = true;
        }

        if (leftPairValue >= 0 && leftPairX >= 0) {
            double pairX = tideMarginLeft + leftPairX;
            double gap = Math.abs(xs[0] - pairX);

            curveXs[0] = xs[0] - gap;

            double rate =
                    (double) (leftPairValue - tideMin) /
                            (double) (tideMax - tideMin);

            double pairBaseY =
                    drawableBottom - (drawableHeight * rate);

            curveYs[0] = leftPairIsLow
                    ? pairBaseY + curveOffset
                    : pairBaseY - curveOffset;
        } else {
            curveXs[0] = xs[0] - tideWidth * 0.08;
            curveYs[0] = curveYs[1];
        }

        // 오른쪽 가상점
        int lastPairIndex = pairIndexes[count - 1];

        int rightPairValue;
        int rightPairX;
        boolean rightPairIsLow;

        if (lows[count - 1]) {
            rightPairValue = maxTideList[lastPairIndex];
            rightPairX = maxTideXList[lastPairIndex];
            rightPairIsLow = false;
        } else {
            rightPairValue = minTideList[lastPairIndex];
            rightPairX = minTideXList[lastPairIndex];
            rightPairIsLow = true;
        }

        if (rightPairValue >= 0 && rightPairX >= 0) {
            double pairX = tideMarginLeft + rightPairX;
            double gap = Math.abs(xs[count - 1] - pairX);

            curveXs[count + 1] = xs[count - 1] + gap;

            double rate =
                    (double) (rightPairValue - tideMin) /
                            (double) (tideMax - tideMin);

            double pairBaseY =
                    drawableBottom - (drawableHeight * rate);

            curveYs[count + 1] = rightPairIsLow
                    ? pairBaseY + curveOffset
                    : pairBaseY - curveOffset;
        } else {
            curveXs[count + 1] = xs[count - 1] + tideWidth * 0.08;
            curveYs[count + 1] = curveYs[count];
        }

        java.awt.geom.Path2D path =
                new java.awt.geom.Path2D.Double();

        path.moveTo(curveXs[0], curveYs[0]);

        for (int i = 0; i < curveXs.length - 1; i++) {
            double x1 = curveXs[i];
            double y1 = curveYs[i];

            double x2 = curveXs[i + 1];
            double y2 = curveYs[i + 1];

            double dx = (x2 - x1) * 0.45;

            path.curveTo(
                    x1 + dx, y1,
                    x2 - dx, y2,
                    x2, y2
            );
        }

        Object oldAntialias =
                g.getRenderingHint(
                        java.awt.RenderingHints.KEY_ANTIALIASING
                );

        java.awt.Stroke oldStroke = g.getStroke();
        java.awt.Color oldColor = g.getColor();
        java.awt.Font oldFont = g.getFont();
        java.awt.Shape oldClip = g.getClip();

        g.setRenderingHint(
                java.awt.RenderingHints.KEY_ANTIALIASING,
                java.awt.RenderingHints.VALUE_ANTIALIAS_ON
        );

        g.setStroke(
                new java.awt.BasicStroke(
                        2f,
                        java.awt.BasicStroke.CAP_ROUND,
                        java.awt.BasicStroke.JOIN_ROUND
                )
        );

        g.setColor(new java.awt.Color(0, 60, 130));

        g.setClip(
                tideMarginLeft,
                tideMarginTop,
                tideWidth,
                tideHeight
        );

        g.draw(path);

        g.setClip(oldClip);

        // 아이콘
        for (int i = 0; i < count; i++) {
            java.awt.geom.Ellipse2D circle =
                    new java.awt.geom.Ellipse2D.Double(
                            xs[i] - iconRadius,
                            ys[i] - iconRadius,
                            iconRadius * 2,
                            iconRadius * 2
                    );

            if (lows[i]) {
                g.setColor(new java.awt.Color(40, 130, 220));
                g.fill(circle);
            } else {
                g.setColor(new java.awt.Color(220, 80, 60));
                g.fill(circle);
            }

            g.setColor(new java.awt.Color(255, 255, 255));
            g.setStroke(new java.awt.BasicStroke(2f));
            g.draw(circle);

            String text = lows[i] ? "저" : "고";

            g.setFont(
                    new java.awt.Font(
                            "Dialog",
                            java.awt.Font.BOLD,
                            18
                    )
            );

            java.awt.FontMetrics fm = g.getFontMetrics();

            int tx =
                    (int) (
                            xs[i] -
                                    fm.stringWidth(text) / 2.0
                    );

            int ty =
                    (int) (
                            ys[i] +
                                    fm.getAscent() / 2.0 - 2
                    );

            g.drawString(text, tx, ty);
        }

        g.setRenderingHint(
                java.awt.RenderingHints.KEY_ANTIALIASING,
                oldAntialias
        );

        g.setStroke(oldStroke);
        g.setColor(oldColor);
        g.setFont(oldFont);
        g.setClip(oldClip);
    }
    
    public void process() {
    	
    	System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
    	
		if(!this.initialize()) {
			
			System.out.println("Error : AcimSectorTableGenerator.process -> initialize failed");
			return;
		}
		
		System.out.println("::: Start Get Acim Model File Map :::");
		
		this.generateFcstTable("2024071200");
		
		this.destroy(); // 자원 해제
    }

	public static void main(String[] args) {
		
		DfsTideFcstTableGenerator acim = new DfsTideFcstTableGenerator();
		acim.process(); // 프로세스 실행
		
    }

}
