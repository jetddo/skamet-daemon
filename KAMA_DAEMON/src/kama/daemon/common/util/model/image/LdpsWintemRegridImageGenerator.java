package kama.daemon.common.util.model.image;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.FloatBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class LdpsWintemRegridImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private XMLOutputFactory factory = XMLOutputFactory.newFactory();
	
	private ModelGridUtil modelGridUtil;
	
	private static BufferedImage[][] wtailImageList;
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private final int imageExpandFactor = 100;
	private final int imageResizeFactor = 1;
		
	private double cropTop = 39.63333321;
	private double cropBottom = 29.00000191;
	private double cropLeft = 123.00000191;
	private double cropRight = 134.649999619999988;
	
	private float wTailRatio = 1f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int fontSize = (int)(this.imageExpandFactor * this.imageResizeFactor / 4);
		
	private List<Map<String, Object>> fileInfoList = new ArrayList<Map<String, Object>>();
	
	private final String fontName = "Times New Roman";
	
	public LdpsWintemRegridImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("LdpsWintemImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	public List<Map<String, Object>> generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		Map<String, List<File>> map = new HashMap<String, List<File>>();
		
		System.out.println("LdpsWintemImageGenerator");
		
		System.out.println("\t-> Create Ldps Image Grid List");
		
//		double[] mapBound = new double[]{40, 30, 0, 360};
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("LdpsWintemImageGenerator [ End Create Image ]");
		
		return this.fileInfoList;
	}
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding");
		
		double minLat = maxBoundLonLat.getBottom();
		double maxLat = maxBoundLonLat.getTop();
		double minLon = maxBoundLonLat.getLeft();
		double maxLon = maxBoundLonLat.getRight();
		
		double latTerm = (maxLat - minLat) / (rows-1);
		double lonTerm = (maxLon - minLon) / (cols-1);
		
		Float[][] regridValues = new Float[rows][];
		boolean[][] regridChecks = new boolean[rows][];
		
		for(int j=0 ; j<rows ; j++) {
			
			regridValues[j] = new Float[cols];
			regridChecks[j] = new boolean[cols];
			
			for(int k=0 ; k<cols ; k++) {
				regridValues[j][k] = -999f;
				regridChecks[j][k] = false;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				float originLat = this.latitudeBuffer.get(j * cols + k);
				float originLon = this.longitudeBuffer.get(j * cols + k);
				
				int y = (int)((originLat - minLat) / latTerm);
				int x = (int)((originLon - minLon) / lonTerm);
				
				regridValues[y][x] = values[j][k];
				regridChecks[y][x] = true;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				if(regridChecks[j][k] == false) {
					
					double regridLat = minLat + latTerm * j;
					double regridLon = minLon + lonTerm * k;
					
					if(regridLat >= this.cropBottom && regridLat <= this.cropTop && regridLon >= this.cropLeft && regridLon <= this.cropRight) {
						
						this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(regridLon, regridLat);
						
						BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
						BoundXY boundXY = this.modelGridUtil.getBoundXY();
						
						float originLat = (float)boundLonLat.getTop();
						float originLon = (float)boundLonLat.getLeft();
						
						if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
							regridValues[j][k] = values[boundXY.getTop()][boundXY.getLeft()];
						}	
					}
				}
			}
		}
		
		Map<String, Object> regridData = new HashMap<String, Object>();
		regridData.put("regridValues", regridValues);
		regridData.put("latTerm", latTerm);
		regridData.put("lonTerm", lonTerm);
		
		System.out.println("\t\t-> End Regriding");
		
		return regridData;
	}
	
	
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			this.createWtailImages();
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute [Wintem]");
			
			int timeIndex = Integer.valueOf(fileName.replaceAll("qwumloa_pb", "").replaceAll(".nc", ""));
			
			int timeLength = fileName.contains("pb000") ? 1 : 2;
			
			List<String> heightInfoList = Arrays.asList(new String[]{"1|010|0", "2|020|0", "4|030|0", "5|040|0", "6|050|0", "7|060|1", "8|080|1", "9|100|1"});
			
			// 070, 090 을 위한 데이터 임시 저장
			List<Float[][]> deriveRegridValuesXWindList = new ArrayList<Float[][]>();
			List<Float[][]> deriveRegridValuesYWindList = new ArrayList<Float[][]>();
			List<Float[][]> deriveRegridValuesTempList = new ArrayList<Float[][]>();
			
			double latTerm = 0d;
			double lonTerm = 0d;
			
			for(int i=0 ; i<timeLength ; i++) {
				
				int fcstHour = timeIndex - (timeLength-1) + i;
				
				if(fcstHour < 6 || fcstHour > 24 || fcstHour % 3 != 0) {
					continue;
				}
				
				String[] savePaths = savePath.split("\\/");
				
				Calendar cal = new GregorianCalendar();
				cal.set(Calendar.YEAR, Integer.valueOf(savePaths[savePaths.length-4]));
				cal.set(Calendar.MONTH, Integer.valueOf(savePaths[savePaths.length-3])-1);
				cal.set(Calendar.DAY_OF_MONTH, Integer.valueOf(savePaths[savePaths.length-2]));
				cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(savePaths[savePaths.length-1]));
				
				Date issuedDt = cal.getTime();
				
				cal.add(Calendar.HOUR_OF_DAY, fcstHour);
				
				Date fcstDt = cal.getTime();
				
				boolean isDeriveData = false;
				
				for(int j=0 ; j<24 ; j++) {
					 
					String feet = null;
					
					for(int k=0 ; k<heightInfoList.size() ; k++) {
						
						String[] heightInfos = heightInfoList.get(k).split("\\|");
						
						if(Integer.valueOf(heightInfos[0]) == j) {
							feet = heightInfos[1];
							
							isDeriveData = Integer.valueOf(heightInfos[2]) == 1 ? true : false;
						}
					}
					
					if(feet == null) {
						continue;
					}
					
					System.out.println("\t\t-> Start Read Variable [x-wind, y-wind, temp]");
					
					Variable varXWind = ncFile.findVariable("x-wind");
					Variable varYWind = ncFile.findVariable("y-wind");
					Variable varTemp = ncFile.findVariable("temp");
					
					List<Range> rangeList = new ArrayList<Range>();
					rangeList.add(new Range(i, i));	
					rangeList.add(new Range(j, j));						
					rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
					rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
					
					Float[][] valuesXWind = GridCalcUtil.convertStorageToValues(varXWind.read(rangeList).getStorage(), rows, cols);
					Float[][] valuesYWind = GridCalcUtil.convertStorageToValues(varYWind.read(rangeList).getStorage(), rows, cols);
					Float[][] valuesTemp = GridCalcUtil.convertStorageToValues(varTemp.read(rangeList).getStorage(), rows, cols);
					
					System.out.println("\t\t-> End Read Variable [x-wind, y-wind, temp]");
					
					Map<String, Object> regridDataXWind = getRegridData(valuesXWind, boundLonLat, rows, cols);
					Map<String, Object> regridDataYWind = getRegridData(valuesYWind, boundLonLat, rows, cols);
					Map<String, Object> regridDataTemp = getRegridData(valuesTemp, boundLonLat, rows, cols);
					
					latTerm = (double)regridDataTemp.get("latTerm");
					lonTerm = (double)regridDataTemp.get("lonTerm");
					
					Float[][] regridValuesXWind = (Float[][])regridDataXWind.get("regridValues");
					Float[][] regridValuesYWind = (Float[][])regridDataYWind.get("regridValues");
					Float[][] regridValuesTemp = (Float[][])regridDataTemp.get("regridValues");
					
					if(isDeriveData) {
						deriveRegridValuesXWindList.add(regridValuesXWind);
						deriveRegridValuesYWindList.add(regridValuesYWind);
						deriveRegridValuesTempList.add(regridValuesTemp);	
					}
					
					String imgFileName = fileName.replaceAll("pb[0-9]{3}.nc", "") + "pb" + String.format("%03d", fcstHour) + "_wintem_" + String.format("%02d", j) +".png";
					
					Map<String, Object> fileInfo = new HashMap<String, Object>();
					
					this.fileInfoList.add(fileInfo);
					
					File imageFile = new File(savePath + File.separator + imgFileName);
					
					fileInfo.put("imageFile", imageFile);
					fileInfo.put("feet", feet);
					
					BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
					
					Graphics2D ig2 = bi.createGraphics();
					
					ig2.setFont(new Font(this.fontName, Font.BOLD, this.fontSize));
					ig2.setColor(new Color(0, 0, 255));		
					
					System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
						
					for(int k=0 ; k<rows-1 ; k++) {
						
						for(int l=0 ; l<cols-1 ; l++) {
							
							if(k % 35 != 0 || l % 25 != 0) {
								continue;
							}
							
							float u = regridValuesXWind[k][l];
							float v = regridValuesYWind[k][l];
							
							if(u == -999 || v == -999) {
								continue;
							}
								
							double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
							double ws = Math.sqrt(u*u + v*v);
							int temp = (int)(regridValuesTemp[k][l] - 273.15);		
								
							int tailIndex = (int)Math.floor(ws * 1.943844 / 5);
							
							if(tailIndex > 19) {
								tailIndex = 19;
							}
							
							double xCoord = boundLonLat.getLeft() + l * lonTerm;							
							double yCoord = boundLonLat.getTop() - k * latTerm;
							
							int xPoint = (int)Math.floor((xCoord - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
							int yPoint = (int)Math.floor(mercatorRatio[(int)Math.floor((yCoord - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor)]);
							
//							ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint, yPoint, null);
							ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint - this.wTailWidth / 2, yPoint - this.wTailHeight / 2, null);
							
							if((int)((wd + 360) % 360) > 90 && (int)((wd + 360) % 360) < 270) {
								ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5);	
							} else {
								ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5 + this.fontSize);	
							}
						}
					}
					
					bi = this.cropImage(bi, boundLonLat, mercatorRatio, cropTop, cropBottom, cropLeft, cropRight);
						
					BufferedImage wintemBaseImg = ImageIO.read(new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wintem_base.png")));
					
					wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
					
					this.appendWintemLabel(wintemBaseImg, issuedDt, fcstDt, feet, timeIndex - (timeLength-1) + i);
					
					wintemBaseImg = Thumbnails.of(wintemBaseImg).imageType(BufferedImage.TYPE_INT_ARGB).forceSize(720, 720).asBufferedImage();
					
					ImageIO.write(wintemBaseImg, "PNG", imageFile);
					
					System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
					
					///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				
					String xmlFileName = fileName.replaceAll("pb[0-9]{3}.nc", "") + "pb" + String.format("%03d", fcstHour) + "_wintem_" + String.format("%02d", j) +".xml";
					
					File xmlFile = new File(savePath + File.separator + xmlFileName);
					
					fileInfo.put("xmlFile", xmlFile);
					
					FileWriter writer = new FileWriter(xmlFile);

					System.out.println("\t\t-> Start Write Xml [" + xmlFile.getAbsolutePath() + "]");
					
					XMLStreamWriter xmlwriter = factory.createXMLStreamWriter(writer);
					
					xmlwriter.writeStartDocument();
				    
				    xmlwriter.writeStartElement("wintem");
				    
				    xmlwriter.writeAttribute("model", "um_ldps_pb");
				    xmlwriter.writeAttribute("timezone", "utc");
				    xmlwriter.writeAttribute("issued_dt", new SimpleDateFormat("yyyyMMddHH").format(issuedDt));
				    xmlwriter.writeAttribute("fcst_dt", new SimpleDateFormat("yyyyMMddHH").format(fcstDt));
				    xmlwriter.writeAttribute("height", "FL"+feet);
				    xmlwriter.writeAttribute("ws_unit", "knot");
				    xmlwriter.writeAttribute("wd_unit", "degree");
				    xmlwriter.writeAttribute("temp_unit", "℃");
				    xmlwriter.writeAttribute("projection", "epsg:4326");
				    xmlwriter.writeAttribute("boundary", cropTop + " " + cropBottom + " " + cropLeft + " " + cropRight);
				    				    
					for(int k=0 ; k<rows-1 ; k++) {
						
						for(int l=0 ; l<cols-1 ; l++) {
							
							if(k % 35 != 0 || l % 25 != 0) {
								continue;
							}
							
							float u = regridValuesXWind[k][l];
							float v = regridValuesYWind[k][l];
							
							if(u == -999 || v == -999) {
								continue;
							}
								
							double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
							double ws = Math.sqrt(u*u + v*v) * 1.943844;
							int temp = (int)(regridValuesTemp[k][l] - 273.15);		
													
							double xCoord = boundLonLat.getLeft() + l * lonTerm;							
							double yCoord = boundLonLat.getTop() - k * latTerm;
							
							if(xCoord < cropLeft || xCoord > cropRight || yCoord < cropBottom || yCoord > cropTop) {
								continue;
							}
						
							xmlwriter.writeStartElement("grid");
							xmlwriter.writeAttribute("lat", yCoord+"");
						    xmlwriter.writeAttribute("lon", xCoord+"");
						    
							xmlwriter.writeStartElement("wd");
							xmlwriter.writeCharacters(wd+"");
							xmlwriter.writeEndElement();
							xmlwriter.writeStartElement("ws");
							xmlwriter.writeCharacters(ws+"");
							xmlwriter.writeEndElement();
							xmlwriter.writeStartElement("temp");
							xmlwriter.writeCharacters(temp+"");
							xmlwriter.writeEndElement();						   
						    
						    xmlwriter.writeEndElement();
						}
					}
					
					xmlwriter.writeEndElement();					
					xmlwriter.writeEndDocument();
					
					writer.flush();
					writer.close();
					
					System.out.println("\t\t-> End Write Xml [" + xmlFile.getAbsolutePath() + "]");
				}				
				
				List<String> deriveHeightInfoList = Arrays.asList(new String[] {"0|070|2", "1|090|2"});
				
				// 여기서부터 파생 고도 이미지 산출
				
				for(int j=0 ; j<deriveHeightInfoList.size() ; j++) {
					 
					String[] heightInfos = deriveHeightInfoList.get(j).split("\\|");
					
					// 추출 인덱스 (0이면 0과1 로 계산)
					int index = Integer.valueOf(heightInfos[0]);
					
					// 산출 비율 (2면 합해서 2로 나눔)
					int ratio = Integer.valueOf(heightInfos[2]);
					
					// feet 정의
					String feet = heightInfos[1];
					
					Float[][] regridValuesXWind1 = deriveRegridValuesXWindList.get(index);
					Float[][] regridValuesYWind1 = deriveRegridValuesYWindList.get(index);
					Float[][] regridValuesTemp1 = deriveRegridValuesTempList.get(index);
					
					Float[][] regridValuesXWind2 = deriveRegridValuesXWindList.get(index+1);
					Float[][] regridValuesYWind2 = deriveRegridValuesYWindList.get(index+1);
					Float[][] regridValuesTemp2 = deriveRegridValuesTempList.get(index+1);
								
					String imgFileName = fileName.replaceAll("pb[0-9]{3}.nc", "") + "pb" + String.format("%03d", fcstHour) + "_derivedwintem_" + String.format("%02d", j) +".png";
					
					Map<String, Object> fileInfo = new HashMap<String, Object>();
					
					this.fileInfoList.add(fileInfo);
					
					File imageFile = new File(savePath + File.separator + imgFileName);
					
					fileInfo.put("imageFile", imageFile);
					fileInfo.put("feet", feet);
					
					BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
					
					Graphics2D ig2 = bi.createGraphics();
					
					ig2.setFont(new Font(this.fontName, Font.BOLD, this.fontSize));
					ig2.setColor(new Color(0, 0, 255));		
					
									
					System.out.println("\t\t-> Start Write Derived Image [" + imageFile.getAbsolutePath() + "]");
						
					for(int k=0 ; k<rows-1 ; k++) {
						
						for(int l=0 ; l<cols-1 ; l++) {
							
							if(k % 35 != 0 || l % 25 != 0) {
								continue;
							}
							
							float u = (regridValuesXWind1[k][l] + regridValuesXWind2[k][l])/ratio;
							float v = (regridValuesYWind1[k][l] + regridValuesYWind2[k][l])/ratio;
							
							if(u == -999 || v == -999) {
								continue;
							}
								
							double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
							double ws = Math.sqrt(u*u + v*v);
							int temp = (int)(((regridValuesTemp1[k][l] - 273.15) + (regridValuesTemp2[k][l] - 273.15))/ratio);		
								
							int tailIndex = (int)Math.floor(ws * 1.943844 / 5);
							
							if(tailIndex > 19) {
								tailIndex = 19;
							}
							
							double xCoord = boundLonLat.getLeft() + l * lonTerm;							
							double yCoord = boundLonLat.getTop() - k * latTerm;
							
							int xPoint = (int)Math.floor((xCoord - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
							int yPoint = (int)Math.floor(mercatorRatio[(int)Math.floor((yCoord - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor)]);
							
//							ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint, yPoint, null);
							ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint - this.wTailWidth / 2, yPoint - this.wTailHeight / 2, null);
							
							if((int)((wd + 360) % 360) > 90 && (int)((wd + 360) % 360) < 270) {
								ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5);	
							} else {
								ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5 + this.fontSize);	
							}
						}
					}
					
					bi = this.cropImage(bi, boundLonLat, mercatorRatio, cropTop, cropBottom, cropLeft, cropRight);
						
					BufferedImage wintemBaseImg = ImageIO.read(new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wintem_base.png")));
					
					wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
					
					this.appendWintemLabel(wintemBaseImg, issuedDt, fcstDt, feet, timeIndex - (timeLength-1) + i);
					
					wintemBaseImg = Thumbnails.of(wintemBaseImg).imageType(BufferedImage.TYPE_INT_ARGB).forceSize(720, 720).asBufferedImage();
					
					ImageIO.write(wintemBaseImg, "PNG", imageFile);
					
					System.out.println("\t\t-> End Write Derived Image [" + imageFile.getAbsolutePath() + "]");
					
					///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				
					String xmlFileName = fileName.replaceAll("pb[0-9]{3}.nc", "") + "pb" + String.format("%03d", fcstHour) + "_derivedwintem_" + String.format("%02d", j) +".xml";
					
					File xmlFile = new File(savePath + File.separator + xmlFileName);
					
					fileInfo.put("xmlFile", xmlFile);
					
					FileWriter writer = new FileWriter(xmlFile);

					System.out.println("\t\t-> Start Write Derived Xml [" + xmlFile.getAbsolutePath() + "]");
					
					XMLStreamWriter xmlwriter = factory.createXMLStreamWriter(writer);
					
					xmlwriter.writeStartDocument();
				    
				    xmlwriter.writeStartElement("wintem");
				    
				    xmlwriter.writeAttribute("model", "um_ldps_pb");
				    xmlwriter.writeAttribute("timezone", "utc");
				    xmlwriter.writeAttribute("issued_dt", new SimpleDateFormat("yyyyMMddHH").format(issuedDt));
				    xmlwriter.writeAttribute("fcst_dt", new SimpleDateFormat("yyyyMMddHH").format(fcstDt));
				    xmlwriter.writeAttribute("height", "FL"+feet);
				    xmlwriter.writeAttribute("ws_unit", "knot");
				    xmlwriter.writeAttribute("wd_unit", "degree");
				    xmlwriter.writeAttribute("temp_unit", "℃");
				    xmlwriter.writeAttribute("projection", "epsg:4326");
				    xmlwriter.writeAttribute("boundary", cropTop + " " + cropBottom + " " + cropLeft + " " + cropRight);
				    				    
					for(int k=0 ; k<rows-1 ; k++) {
						
						for(int l=0 ; l<cols-1 ; l++) {
							
							if(k % 35 != 0 || l % 25 != 0) {
								continue;
							}
							
							float u = (regridValuesXWind1[k][l] + regridValuesXWind2[k][l])/ratio;
							float v = (regridValuesYWind1[k][l] + regridValuesYWind2[k][l])/ratio;
							
							if(u == -999 || v == -999) {
								continue;
							}
								
							double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
							double ws = Math.sqrt(u*u + v*v);
							int temp = (int)(((regridValuesTemp1[k][l] - 273.15) + (regridValuesTemp2[k][l] - 273.15))/ratio);		
													
							double xCoord = boundLonLat.getLeft() + l * lonTerm;							
							double yCoord = boundLonLat.getTop() - k * latTerm;
							
							if(xCoord < cropLeft || xCoord > cropRight || yCoord < cropBottom || yCoord > cropTop) {
								continue;
							}
						
							xmlwriter.writeStartElement("grid");
							xmlwriter.writeAttribute("lat", yCoord+"");
						    xmlwriter.writeAttribute("lon", xCoord+"");
						    
							xmlwriter.writeStartElement("wd");
							xmlwriter.writeCharacters(wd+"");
							xmlwriter.writeEndElement();
							xmlwriter.writeStartElement("ws");
							xmlwriter.writeCharacters(ws+"");
							xmlwriter.writeEndElement();
							xmlwriter.writeStartElement("temp");
							xmlwriter.writeCharacters(temp+"");
							xmlwriter.writeEndElement();						   
						    
						    xmlwriter.writeEndElement();
						}
					}
					
					xmlwriter.writeEndElement();					
					xmlwriter.writeEndDocument();
					
					writer.flush();
					writer.close();
					
					System.out.println("\t\t-> End Write Derived Xml [" + xmlFile.getAbsolutePath() + "]");
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private void appendWintemLabel(BufferedImage wintemBaseImg, Date issuedDt, Date fcstDt, String feet, int fcstHour) {

		final int labelWidth = 350;
		final int labelHeight = 160;
		final int lineHeight = 20;
		final int labelTopMargin = 10;		
		final int fontLeftMargin = 10;
		
		SimpleDateFormat labelDateFormat = new SimpleDateFormat("HH \'UTC\' dd MM yyyy"); 
				
		Graphics2D ig2 = wintemBaseImg.createGraphics();
		ig2.setColor(new Color(255, 255, 255));
		ig2.fillRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setStroke(new BasicStroke(3));
		ig2.setColor(new Color(0, 0, 0));
		ig2.drawRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 20));
		ig2.setColor(new Color(0, 0, 0));	
							
		ig2.drawString("ISSUED BY AMO", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*1);
		ig2.drawString("WIND/TEMPERATURE FL " + feet, wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*2);
		ig2.drawString("FIXED TIME PROGNOSTIC CHART", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*3);
		
		ig2.drawString("BASED ON " + labelDateFormat.format(issuedDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*5);
		
		ig2.drawString("VALID " + labelDateFormat.format(fcstDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*4);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 15));
		ig2.drawString("Units: Wind(knots), Temperature(℃)", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*6);		
		ig2.drawString("Temperatures are negative unless prefixed by PS", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin/2 + lineHeight*7);
		
	}
	
	private BufferedImage cropImage(BufferedImage bi, BoundLonLat boundLonLat, double[] mercatorRatio, double top, double bottom, double left, double right) {
				
		int y1 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - top) * this.imageExpandFactor * this.imageResizeFactor)]);
		int y2 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - bottom) * this.imageExpandFactor * this.imageResizeFactor)]);
		int x1 = (int)Math.floor((left - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		int x2 = (int)Math.floor((right - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
		return bi.getSubimage(x1, y1, x2 - x1, y2 - y1);
	}
    
    private void createWtailImages() throws IOException {
		
		if(wtailImageList != null) {
			System.out.println("\t-> Already Arrow Images Created");
			return;
		} else {
			System.out.println("\t-> Create Arrow Image List");	
		}
		
		String wtailImageDir = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wtail/");
		
		String wtailImagePath = wtailImageDir + "/windTail_blue_{ws}/windTail_blue_{ws}_{wd}.png";
		
		wtailImageList = new BufferedImage[20][];
		
		for(int i=0 ; i<wtailImageList.length ; i++) {
			
			wtailImageList[i] = new BufferedImage[36];
			
			for(int j=0 ; j<wtailImageList[i].length ; j++) {
				
				File originImageFile = new File(wtailImagePath.replaceAll("\\{ws\\}", String.valueOf(i+1))
															  .replaceAll("\\{wd\\}", String.valueOf(j*10)));
				
				wtailImageList[i][j] = Thumbnails.of(originImageFile)
						.imageType(BufferedImage.TYPE_INT_ARGB)
						.size(this.wTailWidth, this.wTailHeight).asBufferedImage();
			}
		}
	}
}
