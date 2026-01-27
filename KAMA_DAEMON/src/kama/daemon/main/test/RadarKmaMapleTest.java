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
import java.util.Date;

import javax.imageio.ImageIO;

import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;

import com.google.common.io.ByteSource;

public class RadarKmaMapleTest {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private RadarHeader radarHeader;
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_MAPLE);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	private float[][] readRadarLayerRawData(EndianDataInputStream dis) throws IOException {
		
		float[][] radarLayerRawData = new float[this.radarHeader.ny][];
		
		byte[] buffer = new byte[this.radarHeader.nx * 4];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRawData[i] = new float[this.radarHeader.nx];
			
			dis.readFully(buffer);
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRawData[i][j] = ByteBuffer.wrap(buffer, j * 4, 4).order(ByteOrder.LITTLE_ENDIAN).getFloat();
			}
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Read Radar Layer Raw Data Size : " + (this.radarHeader.ny * this.radarHeader.nx) + " bytes");
		
		return radarLayerRawData;
	}
	
	private void setLambertEnv() {
		
        for(int i=0 ; i<this.radarHeader.ny ; i++) {
        	
        	for(int j=0 ; j<this.radarHeader.nx ; j++) {
        	
        		PointF coord = this.lambert.lambertToWgs84(j, i);
        		
        		this.maxLat = Math.max(this.maxLat, coord.y);
        		this.maxLon = Math.max(this.maxLon, coord.x);
        		this.minLat = Math.min(this.minLat, coord.y);
        		this.minLon = Math.min(this.minLon, coord.x);
        	}
        }
        
        this.latTerm = (this.maxLat - this.minLat) / (this.radarHeader.ny-1);
        this.lonTerm = (this.maxLon - this.minLon) / (this.radarHeader.nx-1);
        
        System.out.println("maxLat : " + this.maxLat);
        System.out.println("maxLon : " + this.maxLon);
        System.out.println("minLat : " + this.minLat);
        System.out.println("minLon : " + this.minLon);
	}
	
	private float[][] regridRadarLayer(float[][] radarLayerRawData) {
		
		float[][] radarLayerRegridData = new float[this.radarHeader.ny][];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRegridData[i] = new float[this.radarHeader.nx];
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRegridData[i][j] = -999f;
			}
		}
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				
				PointF coord = lambert.lambertToWgs84(j, i);
                int y = (int) ((coord.y - minLat) / latTerm);
                int x = (int) ((coord.x - minLon) / lonTerm);

                radarLayerRegridData[y][x] = radarLayerRawData[i][j];
			}
		}
		
		return radarLayerRegridData;
	}
	
	@SuppressWarnings("unused")
	public void parseRadarFile(String radarFilePath) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA MAPLE Radar Image Converter :::::");
		
		try {
			
			byte[] gZipBuffer = new GZipTgzReader(radarFilePath).readAllBytes();
			
			EndianDataInputStream dis = new EndianDataInputStream(ByteSource.wrap(gZipBuffer).openStream());
			dis.order(ByteOrder.LITTLE_ENDIAN);
			
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA MAPLE Radar Header Info :::::");
			this.parseRadarHeaderInfo(dis);
			
			this.setLambertEnv();
		
			System.out.println(this.radarHeader);

			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA MAPLE Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<=this.radarHeader.nforecasts ; i++) {	
				
				if(true) {
					
					float[][] radarLayerRegridData = this.regridRadarLayer(this.readRadarLayerRawData(dis));
						
					this.createImage(radarLayerRegridData, i);	
				} else {
					dis.skipBytes(this.radarHeader.nx * this.radarHeader.ny * 4);
				}
			}
		
			dis.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA MAPLE Radar Image Converter :::::");
	}
	
	private Color getColor(float data) {
		
		if(data == -999f) {
			return null;
		}
		
		data = data/100f;
		
		if(data > 0.0f && data < 0.1f) {
			return new Color(0,200,255);
		} else if(data >= 0.1f && data < 0.5f) {
			return new Color(0,155,245);
		} else if(data >= 0.5f && data < 1.0f) {
			return new Color(0,51,245);
		} else if(data >= 1.0f && data < 2.0f) {
			return new Color(0,255,0);
		} else if(data >= 2.0f && data < 3.0f) {
			return new Color(0,190,0);
		} else if(data >= 3.0f && data < 4.0f) {
			return new Color(0,140,0);
		} else if(data >= 4.0f && data < 5.0f) {
			return new Color(0,90,0);
		} else if(data >= 5.0f && data < 6.0f) {
			return new Color(255,255,0);
		} else if(data >= 6.0f && data < 7.0f) {
			return new Color(255,220,31);
		} else if(data >= 7.0f && data < 8.0f) {
			return new Color(249,205,0);
		} else if(data >= 8.0f && data < 9.0f) {
			return new Color(224,185,0);
		} else if(data >= 9.0f && data < 10.0f) {
			return new Color(204,170,0);
		} else if(data >= 10.0f && data < 15.0f) {
			return new Color(255,102,0);
		} else if(data >= 15.0f && data < 20.0f) {
			return new Color(255,50,0);
		} else if(data >= 20.0f && data < 25.0f) {
			return new Color(210,0,0);
		} else if(data >= 25.0f && data < 30.0f) {
			return new Color(180,0,0);
		} else if(data >= 30.0f && data < 40.0f) {
			return new Color(224,169,255);
		} else if(data >= 40.0f && data < 50.0f) {
			return new Color(201,105,255);
		} else if(data >= 50.0f && data < 60.0f) {
			return new Color(179,41,255);
		} else if(data >= 60.0f && data < 70.0f) {
			return new Color(147,0,228);
		} else if(data >= 70.0f && data < 80.0f) {
			return new Color(179,180,222);
		} else if(data >= 80.0f && data < 90.0f) {
			return new Color(76,78,177);
		} else if(data >= 90.0f && data < 110.0f) {
			return new Color(0,3,144);
		} else if(data >= 110.0f) {
			return new Color(51,51,51);	
		} else {
			return null;
		}
	}
	
	private void createImage(float[][] radarLayerRegridData, int index) {
		
		String imageFilePath = "C:/Users/koast/Desktop/asdfasdf/radar_kma_maple_" + String.format("%03d", index)  + ".png";
		
		try {
						
			BufferedImage bi = new BufferedImage(this.radarHeader.nx, this.radarHeader.ny, BufferedImage.TYPE_INT_ARGB);	
			
			Graphics2D graphics2D = bi.createGraphics();
			
			for(int i=0 ; i<this.radarHeader.ny ; i++) {
				
				for(int j=0 ; j<this.radarHeader.nx ; j++) {
					
	               Color color = this.getColor(radarLayerRegridData[i][j]);
	               
	               if(color != null) {
	            	
	            	   graphics2D.setColor(color);
	            	   graphics2D.drawLine(j, this.radarHeader.ny - i - 1, j, this.radarHeader.ny - i - 1);	
	               }
				}
			}
			
			ImageIO.write(bi, "PNG", new File(imageFilePath));		
			
		} catch (IOException e) {
			
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFilePath);
	}

	public static void main(String[] args) {
		
		RadarKmaMapleTest radarTest = new RadarKmaMapleTest();
		
		radarTest.parseRadarFile("C:/Users/koast/Desktop/asdfasdf/qpf_ram_10m_maple_202001071200.bin.gz");
	}

	private void parseRadarHeaderInfo(EndianDataInputStream dis) throws Exception {
		
		radarHeader = new RadarHeader();
		
		for(int i=0 ; i<radarHeader.forecast_periods.length ; i++) {
			radarHeader.forecast_periods[i] = dis.readInt();
		}
		
		radarHeader.nforecasts = dis.readInt();
		
		radarHeader.year = dis.readInt();
		radarHeader.month = dis.readInt();
		radarHeader.day = dis.readInt();
		radarHeader.hours = dis.readInt();
		radarHeader.minutes = dis.readInt();
		
		radarHeader.tm_str = String.format("%d%02d%02d%02d%02d", radarHeader.year, radarHeader.month, radarHeader.day, radarHeader.hours, radarHeader.minutes);
		
		radarHeader.nx = dis.readInt();
		radarHeader.ny = dis.readInt();
		
		radarHeader.clat = dis.readFloat();
		radarHeader.clon = dis.readFloat();
		radarHeader.yminl = dis.readFloat();
		radarHeader.ymaxl = dis.readFloat();
		radarHeader.xminl = dis.readFloat();
		radarHeader.xmaxl = dis.readFloat();
		
		radarHeader.forecast_period = dis.readInt();
		radarHeader.maptype = dis.readInt();
	}
	
	@SuppressWarnings("unused")
	private class RadarHeader {
		
		public int[] forecast_periods = new int[10];
		public int nforecasts;
		
		public int year;
		public int month;
		public int day;
		public int hours;
		public int minutes;
		public String tm_str;
		public int nx;
		public int ny;
		public float clat;
		public float clon;
		public float yminl;
		public float ymaxl;
		public float xminl;
		public float xmaxl;
		public int forecast_period;
		public int maptype;
	
		@Override
		public String toString() {
			
			String str = "---------- HEADER INFO ----------\n";
			
			Field[] fields = this.getClass().getFields();
			
			for(Field field : fields) {
				
				Class<?> clazz = field.getType();
				
				if(clazz.isArray()) {
					continue;
				}
				
				try {
					
					str += field.getName() + " -> " + field.get(this) + "\n";
					
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			str += "---------- HEADER INFO ----------";
			
			return str;
		}
	}
}
