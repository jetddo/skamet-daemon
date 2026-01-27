package kama.daemon.model.observation.adopt.RDR.proc;

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

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.EndianDataInputStream;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.Lambert.GRID_PARAM_TYPE;
import kama.daemon.common.util.converter.PointF;

import com.google.common.io.ByteSource;

public class RadarKmaHciConveter {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private RadarHeader radarHeader;
	
	private Lambert lambert = new Lambert(GRID_PARAM_TYPE.RDR_KMA_HCI);
	private double maxLat = -1;
	private double maxLon = -1;
	private double minLat = 91;
	private double minLon = 181;
	private double latTerm, lonTerm;
	
	public RadarKmaHciConveter(DaemonSettings daemonSettings, ProcessorInfo processorInfo) {
		this.daemonSettings = daemonSettings;
		this.processorInfo = processorInfo;
	}
	
	private float[][] readRadarLayerRawData(EndianDataInputStream dis) throws IOException {
		
		float[][] radarLayerRawData = new float[this.radarHeader.ny][];
		
		byte[] buffer = new byte[this.radarHeader.nx * 1];
		
		for(int i=0 ; i<this.radarHeader.ny ; i++) {
			
			radarLayerRawData[i] = new float[this.radarHeader.nx];			
			
			dis.readFully(buffer);
			
			for(int j=0 ; j<this.radarHeader.nx ; j++) {
				radarLayerRawData[i][j] = ByteBuffer.wrap(buffer, j * 1, 1).order(ByteOrder.LITTLE_ENDIAN).get();
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
	public void parseRadarFile(File radarGZipFile) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start KMA HCI Radar Image Converter :::::");
		
		try {
			
			byte[] gZipBuffer = new GZipTgzReader(radarGZipFile.getAbsolutePath()).readAllBytes();
		
			EndianDataInputStream dis = new EndianDataInputStream(ByteSource.wrap(gZipBuffer).openStream());
			dis.order(ByteOrder.LITTLE_ENDIAN);			
			
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA HCI Radar Header Info :::::");
			this.parseRadarHeaderInfo(dis);
			
			this.setLambertEnv();
			
			System.out.println(this.radarHeader);

			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Parse KMA HCI Radar Layer Raw Data :::::");
			
			for(int i=0 ; i<this.radarHeader.nz ; i++) {	
				
				if(i == 28) {
					
					float[][] radarLayerRegridData = this.regridRadarLayer(this.readRadarLayerRawData(dis));		
					
					this.createImage(radarLayerRegridData, radarGZipFile.getName(), i);
					
				} else {
					dis.skipBytes(this.radarHeader.nx * this.radarHeader.ny);
				}
			}
		
			dis.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End KMA HCI Radar Image Converter :::::");
	}
	
	private Color getColor(float data) {
		
		switch((int)data) {
		
		case 1: // 구름(CL)
			return new Color(204,255,204);
		case 2: // 이슬비(DRZ)
			return new Color(153,204,255);
		case 3: // 약한비(LR)
			return new Color(102,153,255);
		case 4: // 중간비(MR)
			return new Color(53,96,255);
		case 5: // 강한비(HR)
			return new Color(51,51,204);
		case 6: // 우박(HA)
			return new Color(255,0,0);
		case 7: // 비+우박(RH)
			return new Color(255,153,153);
		case 8: // 싸락눈+작은우박(GSH)
			return new Color(255,102,0);
		case 9: // 싸락눈+비(GR)
			return new Color(255,204,204);
		case 10: // 건설(DS)
			return new Color(255,153,255);
		case 11: // 습설(WS)
			return new Color(102,255,255);
		case 12: // 등방 빙정(IC)
			return new Color(245,255,0);
		case 13: // 비등방 빙정(IIC)
			return new Color(245,255,0);
		case 14: // 과냉각수적(SLD)
			return new Color(51,204,204);
		case 51:
			return null;
//			return new Color(128,128,128);
		case -999:
			return null;
		default:
			return null;
		}
	}
	
	private void createImage(float[][] radarLayerRegridData, String fileName, int index) {
		
		File imageFileDir = new File(String.format("%s/RDR_IMG/%s/%s", this.daemonSettings.OutputRootPath(), this.processorInfo.ClassPrefix, DateFormatter.formatDate(processorInfo.FileDateFromNameOriginal, "yyyy/MM/dd")));
		
		if(!imageFileDir.exists() && !imageFileDir.mkdirs()) {
			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Error : Cannot Create Directory ( " + imageFileDir.getAbsolutePath() + " )" );
		}
		
		File imageFile = new File(imageFileDir.getAbsolutePath() + File.separator + String.format("%s_%03d.png", fileName.replaceFirst("[.][^.]+$", ""), index + 1));
        	
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
			
			ImageIO.write(bi, "PNG", imageFile);		
			
		} catch (IOException e) {
			
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Create Image : " + imageFile.getAbsolutePath());
	}

	private void parseRadarHeaderInfo(EndianDataInputStream dis) throws IOException {
		
		radarHeader = new RadarHeader();
		
		radarHeader.version =  dis.readByte();
		radarHeader.ptype = dis.readShort();
		dis.readFully(radarHeader.tm);
		dis.readFully(radarHeader.tm_in);
		this.parseHeaderDate();
		radarHeader.num_stn = dis.readByte();
		radarHeader.map_code = dis.readByte();
		radarHeader.map_etc = dis.readByte();
		radarHeader.nx = dis.readShort();
		radarHeader.ny = dis.readShort();
		radarHeader.nz = dis.readShort();
		radarHeader.dxy = dis.readShort();
		radarHeader.dz = dis.readShort();
		radarHeader.z_min = dis.readShort();
		radarHeader.num_data = dis.readByte();
		radarHeader.dz2 = dis.readShort();
		radarHeader.z_min2 = dis.readShort();
		radarHeader.data_out = dis.readShort();
		radarHeader.data_in = dis.readShort();
		radarHeader.data_min = dis.readShort();
		radarHeader.data_minus = dis.readShort();
		radarHeader.data_scale = dis.readShort();
		radarHeader.data_unit = dis.readByte();
		
		dis.skipBytes(16); // skip etc
	}
	
	private void parseHeaderDate() {
		
		int yy, mm, dd, hh, mi, ss;
		
		yy = radarHeader.tm[1] << 8 | radarHeader.tm[0] & 0xFF;
		mm = radarHeader.tm[2];
		dd = radarHeader.tm[3];
		hh = radarHeader.tm[4];
		mi = radarHeader.tm[5];
		ss = radarHeader.tm[6];
		
		radarHeader.tm_str = String.format("%d%02d%02d%02d%02d%02d", yy, mm, dd, hh, mi, ss);
		
		yy = radarHeader.tm_in[1] << 8 | radarHeader.tm_in[0] & 0xFF;
		mm = radarHeader.tm_in[2];
		dd = radarHeader.tm_in[3];
		hh = radarHeader.tm_in[4];
		mi = radarHeader.tm_in[5];
		ss = radarHeader.tm_in[6];
		
		radarHeader.tm_in_str = String.format("%d%02d%02d%02d%02d%02d", yy, mm, dd, hh, mi, ss);
	}
	
	@SuppressWarnings("unused")
	private class RadarHeader {
		
		public byte version; //포멧버전
		public short ptype; //생산Product
		public byte[] tm = new byte[7]; //레이더 관측시각
		public String tm_str; //레이더 관측시각 yyyyMMddHHmmss
		public byte[] tm_in = new byte[7]; //합성자료 생성시각
		public String tm_in_str; //합성자료 생성시각 yyyyMMddHHmmss
		public byte num_stn; //합성에 사용된 레이더 지점수 
		public byte map_code; //지도정보 코드
		public byte map_etc; //기타 지도 정보코드(예비)
		public short nx; //X축 격자점수
		public short ny; //Y축 격자점수
		public short nz; //Z축 격자점수
		public short dxy; //격자점간의 수평거리(m)
		public short dz; //격자점간의 수직거리(m) (nz=1이면 dz=0)
		public short z_min; //nz > 1인 경우, 최저고도값(m) (nz <= 1이면 0)		
		public byte num_data; //(nx*ny*nz)를 1개 자료블럭으로 했을때, 저장된 자료블럭수
		public short dz2; //2번째 단계의 수직격자점간 거리(m)
		public short z_min2; //2번째 단계의 시작 고도값(m)
		public short data_out; //레이더 관측영역밖 NULL값
		public short data_in; //레이더 영역내 비관측영역 NULL값
		public short data_min; //관측영역내 표시를 위한 최소값
		public short data_minus; //저장된 값에서 이 값을 먼저 뺌
		public short data_scale; //빼고 난 다음에 이 값으로 나누어서 실값을 찾음
		public byte data_unit; //위 방식으로 해독한 후에 단위 코드		
		short[] etc = new short[8]; //예비
	
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
