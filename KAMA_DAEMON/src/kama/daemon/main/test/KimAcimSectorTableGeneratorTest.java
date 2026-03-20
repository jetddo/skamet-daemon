package kama.daemon.main.test;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointLonLat;
import kama.daemon.common.util.model.PointXY;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimAcimSectorTableGeneratorTest {
	
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
	private List<NetcdfDataset> getAcimModelFileList() {

		List<NetcdfDataset> modelFileList = new ArrayList<NetcdfDataset>();
		
		// 이부분에서 issuedTm 을 분석하여 모델데이터 파일 셋을 전달해야함
		// 실행되는 시간 시점으로부터 풀세트가 있는 Acim 모델 파일의 issuedTm 을 구한다
		// 아마 디비연동으로 수행하면 될듯?
		// 일단 issuedTm 을 구했다 치고 구현함
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용
		
		try {
			
			Date issuedTm = sdf.parse("2026031800");  

			String modelFilePath = "F:/data/datastore/KIM_ACIM_CNVT/" + sdf2.format(issuedTm); // 모델 파일 경로 설정
			
			File modelDir = new File(modelFilePath);

			if (!modelDir.exists() || !modelDir.isDirectory()) {
				System.err.println("모델 파일 디렉토리가 존재하지 않거나 디렉토리가 아닙니다: " + modelFilePath);
				return null;
			}

			for (File file : modelDir.listFiles()) {
				if (file.isFile() && file.getName().startsWith("amo_kimg_acim_cnvt_f")
						&& file.getName().contains(sdf.format(issuedTm))) {
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(file.getAbsolutePath(), null);
					modelFileList.add(ncFile);
				}
			}
			
			Collections.sort(modelFileList, (f1, f2) -> {
				
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
            });
			
			return modelFileList;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return modelFileList;
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
	
	private void generateSectorTable() {
		
		String latPath = "F:\\data\\datastore\\grid\\kim_gktg_lat.bin";
		String lonPath = "F:\\data\\datastore\\grid\\kim_gktg_lon.bin";

		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GKTG, ModelGridUtil.Position.MIDDLE_CENTER, latPath, lonPath);
		modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(50, 20, 110, 150); // 한반도 영역 설정
		
		// 섹터 정보 및 폴리곤 좌표 설정
		List<Map<String, Object>> sectorDataList = this.getSectorDataList();
		
		// ACIM 모델 파일 리스트 가져오기
		List<NetcdfDataset> modelFileList = this.getAcimModelFileList();
		
		int fcstHourLength = 39;
		int fcstHourInterval = 3; // 예측 시간 간격 (3시간 단위)
		int fcstHourSize = fcstHourLength / fcstHourInterval + 1; // 예측 시간 크기 (0시간 포함)
		
		if (modelFileList.size() < fcstHourSize) {
			System.err.println("모델 파일이 충분하지 않습니다. 예측 시간 크기: " + fcstHourSize + ", 모델 파일 개수: " + modelFileList.size());
			return;
		}
		
		int width = 1276;
		int height = 810;
		
    	int tableLeftMargin = 50; // 왼쪽 여백
    	int tableTopMargin = 150; // 위쪽 여백
    	int tableRightMargin = 50; // 오른쪽 여백
    	int tableBottomMargin = 230; // 아래쪽 여백
    	
    	int tableWidth = width - tableLeftMargin - tableRightMargin;
    	int tableHeight = height - tableTopMargin - tableBottomMargin;
    	
    	int cellWidth = tableWidth / (fcstHourSize+1);
    	int cellHeight = tableHeight / (sectorDataList.size()+1);
    	
    	int extraWidth = tableWidth % (fcstHourSize+1); // 나머지 계산
    	int extraHeight = tableHeight % (sectorDataList.size()+1); // 나머지 계산
    	
    	int fontSize1 = (int) (cellHeight * 0.5); // 셀 높이에 비례하여 폰트 크기 조정
    	int fontSize2 = (int) (cellHeight * 0.35); // 셀 높이에 비례하여 폰트 크기 조정
    	
    	Font titleFont = new Font("", Font.BOLD, 40); // 제목 폰트 크기 조정
    	Font font1 = new Font("", Font.BOLD, fontSize1);
    	Font font2 = new Font("", Font.BOLD, fontSize2);
    	
    	System.out.println("테이블 크기: " + tableWidth + "x" + tableHeight);
    	System.out.println("셀 크기: " + cellWidth + "x" + cellHeight);
    	System.out.println("섹터 개수: " + sectorDataList.size());
    	System.out.println("예측 시간 크기: " + fcstHourSize);
		
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();
        
        // 배경 설정
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, width, height);
        
        this.setTitleText("대류운 정보", g, width, tableTopMargin - 40, titleFont); // 제목 설정       
        
        
	 
        for (int j = 0; j < fcstHourSize+1; j++) {  
        	
        	NetcdfDataset ncFile = null;
        	Float[][] koreaData = null; // 한반도 영역 데이터
        	
        	// 여기서 한반도의 value 를 미리 추출해둔다
        	
			if (j > 0) {
				
				ncFile = modelFileList.get(j - 1); // 첫 번째 열은 예측 시간 이름이 없음
				
				Variable var = ncFile.findVariable("CCT"); // CLTOP 변수 찾기
				
				koreaData = this.getAcimModelKoreaData(ncFile, modelGridUtil, var); // 한반도 영역 데이터 추출				
			}
        	
			for (int i = 0; i < sectorDataList.size()+1; i++) {
		        	
	        	Map<String, Object> sectorData = null;
	        	
				if (i > 0) {
					sectorData = sectorDataList.get(i - 1); // 첫 번째 행은 섹터 이름이 없음
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
						
						this.setCellText("UTC", g, x, y, cellWidth, cellHeight, font1);
						
					} else {
						
						// 첫 번째 행의 나머지 셀은 예측 시간 표시
						String fcstHourText = String.format("%02d", (j - 1) * fcstHourInterval);

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
            	
            	

                // 셀 테두리 그리기
				g.setColor(Color.BLACK);
				g.setStroke(new BasicStroke(1)); // 선 굵기 5px
                g.drawRect(x, y, currentCellWidth, currentCellHeight);
            }
        }
    	
        // 셀 테두리 그리기
        g.setStroke(new BasicStroke(3)); // 선 굵기 5px
    	g.setColor(Color.BLACK);
    	
        g.drawRect(tableLeftMargin, tableTopMargin, width - tableLeftMargin - tableRightMargin, height - tableTopMargin - tableBottomMargin);  
        
        this.createThresholdInfo(g, width, height, tableLeftMargin, tableBottomMargin, sectorNames, sectorThresholds); // 기준고도 정보 생성
        this.createLegend(g, width, height, tableRightMargin, tableBottomMargin, font1); // 범례 생성
        
        g.dispose(); // Graphics2D 객체 자원 해제

        // 이미지 파일로 저장
        try {
            File outputFile = new File("F:\\KAMA_AAMI\\2025\\table.png");
            ImageIO.write(image, "png", outputFile);
            System.out.println("이미지 저장 완료: " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("이미지 저장 실패: " + e.getMessage());
        }
        
        // 모델 파일 리스트 자원 해제
        this.destroyModelFileList(modelFileList);
	}
	
	private void createThresholdInfo(Graphics2D g, int width, int height, int tableLeftMargin, int tableBottomMargin, String[] sectorNames, String[] sectorThresholds) {
		
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
		
		int infoTopMargin = (height - tableBottomMargin) + 30; // 위쪽 여백
		
		Font font = new Font("", Font.BOLD, 18);
		
        g.setFont(font);
        g.setColor(Color.BLACK);
        
        g.drawString("기준고도:", tableLeftMargin, infoTopMargin + font.getSize()); // 범례 텍스트 위치 조정);
        
		for (int i = 0; i < infoStringList.size(); i++) {
			String infoString = infoStringList.get(i);
			g.drawString(infoString, tableLeftMargin, infoTopMargin + (font.getSize() + 5) * (i + 2)); // 범례 텍스트 위치 조정);
		}
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
			this.setCellText(thresholds[i], g, x+2, legendTopMargin-2, cellWidth, cellHeight, font); // 텍스트 가운데 정렬
			
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
			textWidth = text.length() * font.getSize() / 3 * 2; // 영어는 대략적인 문자 너비 계산
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
		
		g.drawString(text, x + fontLeftMargin, y + fontTopMargin + font.getSize()-1); // 수직 중앙 정렬
	}
	
	private void setTitleText(String text, Graphics2D g, int width, int tableTopMargin, Font font) {
        
        g.setFont(font);
        g.setColor(Color.BLACK);
       
        int leftMargin = (width - text.length() * font.getSize()) /2;
        
        g.drawString(text, leftMargin, tableTopMargin/3*2);
    }
	
	private void setIssuedTmText(String text, Graphics2D g, int width, int tableTopMargin, Font font) {
        
        g.setFont(font);
        g.setColor(Color.BLACK);
       
        int leftMargin = (width - text.length() * font.getSize()) /2;
        
        g.drawString(text, leftMargin, tableTopMargin/3*2);
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

	public static void main(String[] args) {
		
		KimAcimSectorTableGeneratorTest acim = new KimAcimSectorTableGeneratorTest();
		
		acim.generateSectorTable();
    }

}
