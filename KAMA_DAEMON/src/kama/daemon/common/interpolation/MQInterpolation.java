package kama.daemon.common.interpolation;

import java.util.List;

public class MQInterpolation {

	// 관측지점 정보
	private ValuePoint[] valuePoints;
	
	// 객관분석 결과
	private float[][] g;	
	
	// 세로 격자 수
	private int ny;	
	// 가로 격자 수
	private int nx;
	
	private int num_stn;
	
	private double TINY = 1.0e-20;
	private int MAX_STN = 10000;
	private double mp = 0.0005; // MQ: 파라미터	
    private double mq_sm = 1.0; // MQ: Smoothing parameter
    
    public MQInterpolation(int nx, int ny, List<ValuePoint> valuePointList) {
    	
    	this.nx = nx;
    	this.ny = ny;
    	
    	this.g = new float[ny+1][nx+1];   
    	
    	this.valuePoints = valuePointList.toArray(new ValuePoint[valuePointList.size()]);
    	this.num_stn = valuePointList.size();
    }
	
	/**
	 * 객관분석
	 * @return
	 */
	public float[][] interpolate() {
		
        float[] x = new float[this.MAX_STN];
        float[] y = new float[this.MAX_STN];
        double[] v = new double[this.MAX_STN];
        double[] s = new double[this.MAX_STN];
        
        float data_min = 99999;
        float data_max = -99999;
        float data_avg = 0;
        
        for (int k = 0; k<this.num_stn; k++) {
        	
        	float data = this.valuePoints[k].v;

            if (data_min > data) {
                data_min = data;
            }

            if (data_max < data) {
                data_max = data;
            }

            data_avg += data;
        }

        data_avg /= (float) this.num_stn;
         
        for (int k = 0; k < this.num_stn; k++) {
        	
            x[k] = this.valuePoints[k].x / this.nx;
            y[k] = this.valuePoints[k].y / this.ny;

            if (data_max != data_min) {
            	
                v[k] = (this.valuePoints[k].v - data_min) / (data_max - data_min);
                s[k] = (this.mq_sm * this.num_stn) * (0.1 / (data_max - data_min)) * (0.1 / (data_max - data_min));
                
            } else {
            	
                v[k] = this.valuePoints[k].v;
                s[k] = this.mq_sm * this.num_stn * (0.1 * 0.1);
            }
        }
        
        this._mq(x, y, v, s);
        
        for(int j=0 ; j<=this.ny ; j++) {
        	
        	for(int i=0 ; i<=this.nx ; i++) {
        		
        		if(this.g[j][i] > -90) {
        			this.g[j][i] = this.g[j][i]*(data_max - data_min) + data_min;
        		}
        	}
        }
        
		return this.g;
	}
	
	private void _mq(float[] x, float[] y, double[] v, double[] s) {
		
		 double dx = 1.0 / this.nx;
		 double dy = 1.0 / this.ny;
		 double cc = 1.0 / (this.mp * this.mp);
		 
		 double[][] q = new double[this.num_stn][this.num_stn];
		 int[] indx = new int[this.num_stn];
		 
		 for(int i=0 ; i<this.num_stn ; i++) {
			 
			 for(int j=i+1 ; j<this.num_stn ; j++) {				
				 q[i][j] = this.hydrof((double)x[i], (double)y[i], (double)x[j], (double)y[j], cc);				 
			 }
		 }
		 
		 for(int i=0 ; i<this.num_stn ; i++) {
			 
			 for(int j=0 ; j<i ; j++) {				 
				 q[i][j] = q[j][i];
			 }
		 }
		 
		 for(int i=0 ; i<this.num_stn ; i++) {
			 q[i][i] = -1.0 + s[i];
		 }
		 
		 this.ludcmp(q, this.num_stn, indx);
		 
		 this.lubksb(q, this.num_stn, indx, v);		 
		 
		 for(int j=0 ; j<=ny ; j++) {
			 
			 double y1 = dy*j;
			 
			 for(int i=0 ; i<=nx ; i++) {
				 
				 double x1 = dx*i;
				 
				 if(this.g[j][i] > -90) {
					 
					 double q1 = 0;
					 
					 for(int k=0 ; k<this.num_stn ; k++) {						 
						 q1 += v[k]*this.hydrof(x1, y1, (double)x[k], (double)y[k], cc);						 
					 }
					 
					 this.g[j][i] = (float)q1;
					 
				 } else {					 
					 this.g[j][i] = -99.9f;
				 }
			 }
		 }
	}
	
	private double hydrof(double x1, double y1, double x2, double y2, double cc) {
		
        double dx = x2 - x1;
        double dy = y2 - y1;
		double hf = -Math.sqrt((dx * dx + dy * dy) * cc + 1.0);

        return hf;
    }
	
	private void ludcmp(double[][] a, int n, int[] indx) {
		
		double[] vv = new double[n];
		Float d = new Float(1.0);
		
		int imax = 0;
		
		for(int i=0 ; i<n ; i++) {
			
			double big = 0;
			
			for(int j=0 ; j<n ; j++) {
				
				if(Math.abs(a[i][j]) > big) {
					big = Math.abs(a[i][j]);
				}
			}
			
			vv[i] = 1.0/big;
		}
		
		for(int j=0 ; j<n ; j++) {
			
			for(int i=0 ; i<j ; i++) {
			
				double sum = a[i][j];
				
				for(int k=0 ; k<i ; k++) {					
					sum -= a[i][k]*a[k][j];
				}
				
				a[i][j] = sum;
			}
			
			double big = 0;
			
			for(int i=j ; i<n ; i++) {
				
				double sum = a[i][j];
				
				for(int k=0 ; k<j ; k++) {					
					sum -= a[i][k]*a[k][j];
				}
				
				a[i][j] = sum;
				
				if(vv[i]*Math.abs(sum) >= big) {
					
					big = vv[i]*Math.abs(sum);
					imax = i;
				}				
			}
			
			if(j != imax) {
				
				for(int k=0 ; k<n ; k++) {
					
					double dum = a[imax][k];
					a[imax][k] = a[j][k];
					a[j][k] = dum;
				}
				
				d = -d;
				vv[imax] = vv[j];
			}
			
			indx[j] = imax;
			
			if(a[j][j] == 0) {
				a[j][j] = this.TINY;
			}
			
			if(j != n-1) {
				
				double dum = 1.0/a[j][j];
				
				for(int i=j+1 ; i<n ; i++) {
					a[i][j] *= dum;
				}
			}
		}
	}
	
	private void lubksb(double[][] a, int n, int[] indx, double[] b) {
		
		int ii = -1;
		
		for(int i=0 ; i<n ; i++) {
			
			int ip = indx[i];
			double sum = b[ip];
			b[ip] = b[i];
			
			if(ii >= 0) {
				
				for(int j=ii ; j<=i-1 ; j++) {					
					sum -= a[i][j]*b[j];					
				}
				
			} else if(sum != 0) {
				ii = i;
			}
			
			b[i] = sum;
		}
		
		for(int i=n-1 ; i>=0 ; i--) {
			
			double sum = b[i];
			
			for(int j=i+1 ; j<n ; j++) {
				
				sum -= a[i][j]*b[j];
			}
			
			b[i] = sum/a[i][i];
		}
	}
}
