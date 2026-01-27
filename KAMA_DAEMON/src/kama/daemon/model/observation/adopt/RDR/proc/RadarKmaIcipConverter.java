package kama.daemon.model.observation.adopt.RDR.proc;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.imageio.ImageIO;

import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class RadarKmaIcipConverter {
	
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
	
	public RadarKmaIcipConverter() {	
	}
	
	private byte[][] readRadarLayerIcipRawData(Variable var, int index) throws IOException, InvalidRangeException {
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(0, ny-1));
		rangeList.add(new Range(0, nx-1));
		
		byte[] storage = (byte[])var.read(rangeList).getStorage();
		
		byte[][] radarLayerRawData = new byte[this.ny][]; 
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRawData[i] = new byte[this.nx];			
			
			for(int j=0 ; j<this.nx ; j++) {
				
				byte v = storage[ny * i + j];
				
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
	
	private byte[][] regridRadarLayer(byte[][] radarLayerRawData) {
		
		byte[][] radarLayerRegridData = new byte[this.ny][];
		
		for(int i=0 ; i<this.ny ; i++) {
			
			radarLayerRegridData[i] = new byte[this.nx];
			
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
	
	public void parseRadarFile(File radarNcFile, Date fileDate, String storePath) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA ICIP Radar Image Converter :::::");
		
		try {
					
			this.setLambertEnv();
	
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA ICIP Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<this.nz ; i++) {	
				
				if(true) {
					
					NetcdfDataset ncFile = NetcdfDataset.acquireDataset(radarNcFile.getAbsolutePath(), null);
		        	
					Variable var = ncFile.findVariable("ICIv3");
					
					byte[][] radarLayerReshapeData = this.regridRadarLayer(this.readRadarLayerIcipRawData(var, i));	
					
					this.createImage(radarLayerReshapeData, storePath, radarNcFile.getName(), fileDate, i);

		            ncFile.close();				
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA ICIP Radar Image Converter :::::");
	}
	
	private void createImage(byte[][] radarLayerReshapeData, String storePath, String fileName, Date fileDate, int index) {
		
		try {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd");
			
			File imageFileDir = new File(storePath + File.separator + sdf2.format(fileDate));
			
			if(!imageFileDir.exists() && !imageFileDir.mkdirs()) {
				System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Error : Cannot Create Directory ( " + imageFileDir.getAbsolutePath() + " )" );
			}
			
			File imageFile = new File(imageFileDir.getAbsolutePath() + File.separator + String.format("%s_%03d.png", fileName, index + 1));
						
			BufferedImage bi = new BufferedImage(this.nx, this.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.ny ; i++) {
				
				for(int j=0 ; j<this.nx ; j++) {
				
					byte v = radarLayerReshapeData[i][j];
					
					 Color color = null;
					 
					 switch((int)v) {
					 
						case 1:
							color = new Color(201,233,255);
							break;
						case 2:
							color = new Color(233,168,238);
							break;
						case 3:
							color = new Color(169,70,176);
							break;
					 
					 }
		               
		             if(color != null) {
		            	
		            	 graphics2D.setColor(color);
		            	 graphics2D.drawLine(j, this.ny - i - 1, j, this.ny - i - 1);	
		             }					
				}
			}
			
			ImageIO.write(bi, "PNG", imageFile);	
			
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFile.getAbsolutePath());
			
		} catch (Exception  e) {
			e.printStackTrace();
		}
	}
}
