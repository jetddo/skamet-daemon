package kama.daemon.common.util.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * 모델 파일 추출 모듈
 * @author Jetddo
 *
 */

public class ModelBinaryExtractor {
		
	public float[][] extractGridsFloat(String filePath, ModelGridUtil modelGridUtil, boolean reverse) {
		
		try {
						
			final int FLOAT_BYTE = 4;
			
			BoundXY boundXY = modelGridUtil.getBoundXY();
				
			byte[][] rawGrids = new byte[modelGridUtil.getRows()][modelGridUtil.getCols()*FLOAT_BYTE];
			float[][] valueGrids = new float[modelGridUtil.getRows()][modelGridUtil.getCols()];
			
			RandomAccessFile r = new RandomAccessFile(filePath, "r");
			
			if(reverse) {
				
				long pos = (modelGridUtil.getModelHeight() - boundXY.getBottom() - 1)*modelGridUtil.getModelWidth()*FLOAT_BYTE;
				
				for(int i=0 ; i<modelGridUtil.getRows() ; i++) {
					
					if(i==0) {
						pos += boundXY.getLeft()*FLOAT_BYTE;	
					} else {
						pos += (modelGridUtil.getModelWidth()-boundXY.getRight()-1)*FLOAT_BYTE;
						pos -= (modelGridUtil.getModelWidth()*2)*FLOAT_BYTE;
						pos += boundXY.getLeft()*FLOAT_BYTE;
					}
					
					r.seek(pos);			
					r.readFully(rawGrids[i]);
					pos += rawGrids[i].length;
				}
				
			} else {
			
				long pos = boundXY.getBottom()*modelGridUtil.getModelWidth()*FLOAT_BYTE;
				
				for(int i=0 ; i<modelGridUtil.getRows() ; i++) {
					
					if(i==0) {
						pos += boundXY.getLeft()*FLOAT_BYTE;	
					} else {
						pos += (modelGridUtil.getModelWidth()-boundXY.getRight()-1)*FLOAT_BYTE+boundXY.getLeft()*FLOAT_BYTE;
					}
					
					r.seek(pos);			
					r.readFully(rawGrids[i]);
					pos += rawGrids[i].length;
				}
			}
			
			for(int i=0 ; i<modelGridUtil.getRows() ; i++) {			
				for(int j=0, k=0 ; k<modelGridUtil.getCols() ; j+=FLOAT_BYTE, k++) {					
					valueGrids[i][k] = ByteBuffer.wrap(rawGrids[i], j, FLOAT_BYTE).getFloat();					
				}
			}
			
			r.close();
					
			return valueGrids;
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Float extractPointFloat(String filePath, ModelGridUtil modelGridUtil, boolean reverse) {
			
		try {
		
			final int FLOAT_BYTE = 4;
			
			BoundXY boundXY = modelGridUtil.getBoundXY();
					
			byte[] rawPoint = new byte[FLOAT_BYTE];
			
			RandomAccessFile r = new RandomAccessFile(filePath, "r");
			
			if(reverse) {
				
				long pos = boundXY.getTop()*modelGridUtil.getModelWidth()*FLOAT_BYTE + boundXY.getLeft()*FLOAT_BYTE;
				
				r.seek(pos);
				r.read(rawPoint);
				
			} else {
			
				
				long pos = (modelGridUtil.getModelHeight() - boundXY.getTop() - 1)*modelGridUtil.getModelWidth()*FLOAT_BYTE + boundXY.getLeft()*FLOAT_BYTE;
				
				r.seek(pos);
				r.read(rawPoint);
			}
			
			r.close();
	
			float value = ByteBuffer.wrap(rawPoint, 0, FLOAT_BYTE).getFloat();		
	
			return value;
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
