package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.imageio.ImageIO;

import com.google.common.io.ByteSource;

import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import kama.daemon.common.util.converter.PointF;

public class RadarKmaHailTest {
	
private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_HCI);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	private int nz = 1;
	private int ny = 2049;
	private int nx = 2049;
	
	private float[][] readRadarLayerHailRawData(Variable var, int index) throws IOException, InvalidRangeException {
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(0, ny-1));
		rangeList.add(new Range(0, nx-1));
		
		float[] storage = (float[])var.read(rangeList).getStorage();
		
		float[][] radarLayerRawData = new float[this.ny][]; 
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRawData[i] = new float[this.nx];			
			
			for(int j=0 ; j<this.nx ; j++) {
				
				float v = storage[ny * i + j];
				
				radarLayerRawData[i][j] = v;
			}
		}
		
		return radarLayerRawData;
	}
	
	private void setLambertEnv() {
		
        for(int i=0 ; i<this.ny ; i++) {
        	
        	for(int j=0 ; j<this.nx ; j++) {
        	
        		PointF coord = this.lambert.lambertToWgs84(j, i);
        		
        		this.maxLat = Math.max(this.maxLat, coord.y);
        		this.maxLon = Math.max(this.maxLon, coord.x);
        		this.minLat = Math.min(this.minLat, coord.y);
        		this.minLon = Math.min(this.minLon, coord.x);        		
        	}
        }
        
        this.latTerm = (this.maxLat - this.minLat) / (this.ny-1);
        this.lonTerm = (this.maxLon - this.minLon) / (this.nx-1);
        
        System.out.println("maxLat : " + this.maxLat);
        System.out.println("maxLon : " + this.maxLon);
        System.out.println("minLat : " + this.minLat);
        System.out.println("minLon : " + this.minLon);
	}
	
	private float[][] regridRadarLayer(float[][] radarLayerRawData) {
		
		float[][] radarLayerRegridData = new float[this.ny][];
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRegridData[i] = new float[this.nx];
			
			for(int j=0 ; j<this.nx ; j++) {
				radarLayerRegridData[i][j] = 0x0;
			}
		}
		
		for(int i=0 ; i<this.ny ; i++) {
			
			for(int j=0 ; j<this.nx ; j++) {
				
				PointF coord = lambert.lambertToWgs84(j, i);
                int y = (int) ((coord.y - minLat) / latTerm);
                int x = (int) ((coord.x - minLon) / lonTerm);

                radarLayerRegridData[y][x] = radarLayerRawData[i][j];
			}
		}
		
		return radarLayerRegridData;
	}
	
	public void parseRadarFile(String radarFilePath) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA HAIL Radar Image Converter :::::");
		
		try {
					
			this.setLambertEnv();
	
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA HAIL Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<this.nz ; i++) {	
				
				if(true) {
					
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(radarFilePath, null);
		        	
					Variable var = ncFile.findVariable("Hsign");
					
					float[][] radarLayerReshapeData = this.regridRadarLayer(this.readRadarLayerHailRawData(var, i));	
					
					this.createImage(radarLayerReshapeData, i);

		            ncFile.close();				
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA HAIL Radar Image Converter :::::");
	}
	
	private void createImage(float[][] radarLayerReshapeData, int index) {
		
		String imageFilePath = "F:/KAMA_AAMI/2024/(241203)레이더자료 샘플 전달/radar_kma_hail_" + String.format("%03d", index)  + ".png";
		
		try {
						
			BufferedImage bi = new BufferedImage(this.nx, this.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.ny ; i++) {
				
				for(int j=0 ; j<this.nx ; j++) {
				
					float v = radarLayerReshapeData[i][j];
					
					 Color color = null;
					 
					 switch((int)v) {
					 
						case 1:
							color = new Color(176,224,230);
							break;
						case 2:
							color = new Color(240,230,140);
							break;
						case 3:
							color = new Color(255,69,0);
							break;
						case 4:
							color = new Color(128,0,128);
							break;
					 
					 }
		               
		             if(color != null) {
		            	
		            	 graphics2D.setColor(color);
		            	 graphics2D.drawLine(j, this.ny - i - 1, j, this.ny - i - 1);	
		             }					
				}
			}
			
			ImageIO.write(bi, "PNG", new File(imageFilePath));		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFilePath);
	}
	

	public static void main(String[] args) {
		
		RadarKmaHailTest radarTest = new RadarKmaHailTest();
		
		radarTest.parseRadarFile("F:/KAMA_AAMI/2024/항기청_수신/항기청_수신_20241211/RDR_R2D_HAIL_202412100100.nc");
	}
}
