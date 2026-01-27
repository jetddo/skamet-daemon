package kama.daemon.model.prediction.adopt.DFS.loader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import kama.daemon.model.prediction.adopt.DFS.loader.exception.ParseException;
import kama.daemon.model.prediction.adopt.DFS.loader.section.DFS_GRB1_INF;

public final class DFS_GRB1_Loader {

	private static final boolean DEBUG = false;

	// 버퍼 최대크기
	private static final int MAX_BUFFER_SIZE = 204800;

	private static final int EOF = -1;

	private DFS_GRB1_INF dfs_grb1_inf = new DFS_GRB1_INF();

	private float[][] dfs_grb1_data = new float[DFS_GRB1_Constants.NY][DFS_GRB1_Constants.NX];

	private int sbit = 8;
	private int sbyte = 0;

	public void parse(final File file, final DFS_GRB1_Callback callback) throws FileNotFoundException, IOException {
		try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
			if (parse0(dis, callback) == false) {
				throw new ParseException(String.format("동네예보 파일(%s) 분석이 실패하였습니다.", file.getName()));
			}
		}
	}

	private boolean parse0(final DataInputStream dis, final DFS_GRB1_Callback callback) throws IOException {
		this.sbit = 8;
		this.sbyte = 0;

		byte[] buf = new byte[MAX_BUFFER_SIZE];

		int length;
		while ((length = dfs_grb1_read(dis, buf)) > 0) {
			if (DEBUG) {
				System.out.println(String.format("Start Time = %04d.%02d.%02d.%02d:%02d(UTC) / Forecast Time : +%02dH", this.dfs_grb1_inf.s1.YY, this.dfs_grb1_inf.s1.MM, this.dfs_grb1_inf.s1.DD, this.dfs_grb1_inf.s1.HH, this.dfs_grb1_inf.s1.MI, this.dfs_grb1_inf.s1.P1));
			}

			/**
			 * 자료 해독
			 */
	        if (this.dfs_grb1_inf.s2.nx != DFS_GRB1_Constants.NX || this.dfs_grb1_inf.s2.ny != DFS_GRB1_Constants.NY) {
	        	if (DEBUG) {
	        		System.out.println(" Grid Number is not correted");
	        	}

	        	return false;
	        }

	        float RefV = this.dfs_grb1_inf.s4.minimum;
	        float Scale_D = (float) Math.pow(10.0, this.dfs_grb1_inf.s1.Scale_D);
	        float Scale_E = (float) Math.pow(2.0, this.dfs_grb1_inf.s4.Scale_E);

	        this.sbit = 8;
	        this.sbyte = 0;

	        int i = 0;
	        int j = 0;
	        int v = 0;
	        int n = 0;

	        while ((length - this.sbyte) > 1) {
	            v = bit_decode(buf, this.dfs_grb1_inf.s4.num_bits);

	            j = n / DFS_GRB1_Constants.NX;
	            i = n % DFS_GRB1_Constants.NX;

	            this.dfs_grb1_data[j][i] = ((float)v * Scale_E + RefV) / Scale_D;

	            if (++n >= DFS_GRB1_Constants.NY * DFS_GRB1_Constants.NX) {
	            	break;
	            }
	        }

	        if (DEBUG) {
	        	System.out.println(String.format(" total number = %d, Scale_E = %f, RefV = %f, Scale_D = %f", n, Scale_E, RefV, Scale_D));
	        }

	        if (callback != null && callback.callback(this.dfs_grb1_inf, this.dfs_grb1_data) == false) {
        		return false;
	        }

	        /**
	         * 버퍼를 초기화한다.
	         */
	        dfs_grb1_inf.clear();
	        for (int j1 = 0; j1 < DFS_GRB1_Constants.NY; ++j1) {
	            for (int i1 = 0; i1 < DFS_GRB1_Constants.NX; ++i1) {
	            	 this.dfs_grb1_data[j1][i1] = 0.0f;
	            }
	        }
		}

		if (length < 0 && length != -301) {
			return false;
		}

		return true;
	}

	private int dfs_grb1_read(final DataInputStream dis, final byte[] buf) throws IOException {
		byte[] b = new byte[MAX_BUFFER_SIZE];

	    //
	    //  Start Point Search
	    //
		if (dis.read(b, 0, 4) <= 0) {
			return -301;
		}

		int mode = 0;
		for (int index = 0; index < 254; ++index) {
			String head = new String(b, 0, 4);
			if (head.equals("GRIB") == true) {
				mode = 1;
				break;
			} else {
				byte b1 = dis.readByte();
				if (b1 == EOF) {
					return -302;
				}
				
				for (int i = 0; i < 3; ++i) {
					b[i] = b[i + 1];
				}
				b[3] = b1;
			}
		}

		if (mode != 1) {
			return -303;
		}
		
		this.dfs_grb1_inf.s0.head = "GRIB";

	    //
	    //  Section 0 (IS)
	    //
		if (dis.read(b, 0, 4) <= 0) {
			return -304;
		}

		this.dfs_grb1_inf.s0.length = (((b[0] & 0xff) * 256 + (b[1] & 0xff)) * 256) + (b[2] & 0xff);
		this.dfs_grb1_inf.s0.version = (b[3] & 0xff);

		if (DEBUG) {
			System.out.println("");
			System.out.println("[ Section 0 (IS) ]");
			System.out.println(this.dfs_grb1_inf.s0.head);
			System.out.println(String.format("total length = %d bytes", this.dfs_grb1_inf.s0.length));
			System.out.println(String.format("edition number = %d", this.dfs_grb1_inf.s0.version));
		}

	    //
	    //  Section 1 (PDS)
	    //
		if (dis.read(b, 0, 3) <= 0) {
			return -305;
		}

		this.dfs_grb1_inf.s1.length = (((b[0] & 0xff) * 256 + (b[1] & 0xff)) * 256) + (b[2] & 0xff);

		if (dis.read(b, 3, this.dfs_grb1_inf.s1.length - 3) <= 0) {
			return -306;
		}

		this.dfs_grb1_inf.s1.version = b[3] & 0xff;
		this.dfs_grb1_inf.s1.center = b[4] & 0xff;
		this.dfs_grb1_inf.s1.id = b[5] & 0xff;
		this.dfs_grb1_inf.s1.grid = b[6] & 0xff;
		this.dfs_grb1_inf.s1.flag = b[7] & 0xff;
		this.dfs_grb1_inf.s1.unit = b[8] & 0xff;
		this.dfs_grb1_inf.s1.layer = b[9] & 0xff;
		this.dfs_grb1_inf.s1.height = (b[10] & 0xff) * 256 + (b[11] & 0xff);
		this.dfs_grb1_inf.s1.YY = (b[12] & 0xff) + 2000;
		this.dfs_grb1_inf.s1.MM = b[13] & 0xff;
		this.dfs_grb1_inf.s1.DD = b[14] & 0xff;
		this.dfs_grb1_inf.s1.HH = b[15] & 0xff;
		this.dfs_grb1_inf.s1.MI = b[16] & 0xff;
		this.dfs_grb1_inf.s1.forecast_time_unit = b[17] & 0xff;
		this.dfs_grb1_inf.s1.P1 = b[18] & 0xff;
		this.dfs_grb1_inf.s1.P2 = b[19] & 0xff;
		this.dfs_grb1_inf.s1.time_range = b[20] & 0xff;
		this.dfs_grb1_inf.s1.avg_num = (b[21] & 0xff) * 256 + (b[22] & 0xff);
		this.dfs_grb1_inf.s1.missing_num = b[23] & 0xff;
		this.dfs_grb1_inf.s1.century = b[24] & 0xff;
		this.dfs_grb1_inf.s1.subcenter = b[25] & 0xff;
		this.dfs_grb1_inf.s1.Scale_D = (b[26] & 0xff) * 256 + (b[27] & 0xff);

		if (DEBUG) {
			System.out.println("");
			System.out.println("[ Section 1 (PDS) ]");
			System.out.println(String.format("Section 1 length = %d bytes", this.dfs_grb1_inf.s1.length));
			System.out.println(String.format("Parameter Table Version Number = %d", this.dfs_grb1_inf.s1.version));
			System.out.println(String.format("Identification of center (Table 0 - Part 1) = %d", this.dfs_grb1_inf.s1.center));
			System.out.println(String.format("Generation process ID number (Table A) = %d", this.dfs_grb1_inf.s1.id));
			System.out.println(String.format("Grid Identification = %d", this.dfs_grb1_inf.s1.grid));
			System.out.println(String.format("Flag specifying (Table 1) = %d", this.dfs_grb1_inf.s1.flag));
			System.out.println(String.format("Indicator of parameter and units (Table 2) = %d", this.dfs_grb1_inf.s1.unit));
			System.out.println(String.format("Indicator of type of level or layer (Table 3 & 3a) = %d", this.dfs_grb1_inf.s1.layer));
			System.out.println(String.format("Height, pressure, etc. of the level or layer (Table 3) = %d", this.dfs_grb1_inf.s1.height));
			System.out.println(String.format("YY.MM.DD.HH:MI = %04d.%02d.%02d.%02d:%02d", this.dfs_grb1_inf.s1.YY, this.dfs_grb1_inf.s1.MM, this.dfs_grb1_inf.s1.DD, this.dfs_grb1_inf.s1.HH, this.dfs_grb1_inf.s1.MI));
			System.out.println(String.format("Forecast time unit (table 4) = %d", this.dfs_grb1_inf.s1.forecast_time_unit));
			System.out.println(String.format("P1 - Period of time = %d", this.dfs_grb1_inf.s1.P1));
			System.out.println(String.format("P2 - Period of time = %d", this.dfs_grb1_inf.s1.P2));
			System.out.println(String.format("Time range indicator = %d", this.dfs_grb1_inf.s1.time_range));
			System.out.println(String.format("Number included in average (Table 5) = %d", this.dfs_grb1_inf.s1.avg_num));
			System.out.println(String.format("Number Missing from average = %d", this.dfs_grb1_inf.s1.missing_num));
			System.out.println(String.format("Century of Initial time = %d", this.dfs_grb1_inf.s1.century));
			System.out.println(String.format("Identification of sub-center (Table 0 - Part 2) = %d", this.dfs_grb1_inf.s1.subcenter));
			System.out.println(String.format("Decimal scale factor D = %d", this.dfs_grb1_inf.s1.Scale_D));
		}
		
	    //
	    //  Section 2 (GDS)
	    //
		if (((this.dfs_grb1_inf.s1.flag & 0xff) & 128) == 128) {
			if (dis.read(b, 0, 3) <= 0) {
				return -307;
			}

			this.dfs_grb1_inf.s2.length = (((b[0] & 0xff) * 256 + (b[1] & 0xff)) * 256) + (b[2] & 0xff);

			if (dis.read(b, 3, this.dfs_grb1_inf.s2.length - 3) <= 0) {
				return -308;
			}

			this.dfs_grb1_inf.s2.NV = b[3] & 0xff;
			this.dfs_grb1_inf.s2.PV = b[4] & 0xff;
			this.dfs_grb1_inf.s2.data_type = b[5] & 0xff;
			this.dfs_grb1_inf.s2.nx = (b[6] & 0xff) * 256 + (b[7] & 0xff);
			this.dfs_grb1_inf.s2.ny = (b[8] & 0xff) * 256 + (b[9] & 0xff);
			this.dfs_grb1_inf.s2.lat1 = ((b[10] & 0xff) * 256 + (b[11] & 0xff)) * 256 + (b[12] & 0xff);
			this.dfs_grb1_inf.s2.lon1 = ((b[13] & 0xff) * 256 + (b[14] & 0xff)) * 256 + (b[15] & 0xff);
			this.dfs_grb1_inf.s2.flag = b[16] & 0xff;
			this.dfs_grb1_inf.s2.lov = ((b[17] & 0xff) * 256 + (b[18] & 0xff)) * 256 + (b[19] & 0xff);
			this.dfs_grb1_inf.s2.dx = ((b[20] & 0xff) * 256 + (b[21] & 0xff)) * 256 + (b[22] & 0xff);
			this.dfs_grb1_inf.s2.dy = ((b[23] & 0xff) * 256 + (b[24] & 0xff)) * 256 + (b[25] & 0xff);

			if (DEBUG) {
				System.out.println("");
				System.out.println("[ Section 2 (GDS) ]");
				System.out.println(String.format("Section 2 length = %d bytes", this.dfs_grb1_inf.s2.length));
				System.out.println(String.format("Number of vertical coordinate parameters = %d", this.dfs_grb1_inf.s2.NV));
				System.out.println(String.format("List of vertical coordinate parameters = %d", this.dfs_grb1_inf.s2.PV));
				System.out.println(String.format("Data representation type (Table 6) = %d", this.dfs_grb1_inf.s2.data_type));
				System.out.println(String.format("Number of X-grid = %d", this.dfs_grb1_inf.s2.nx));
				System.out.println(String.format("Number of Y-grid = %d", this.dfs_grb1_inf.s2.ny));
				System.out.println(String.format("Latitude of first grid point = %d", this.dfs_grb1_inf.s2.lat1));
				System.out.println(String.format("Longitude of first grid point = %d", this.dfs_grb1_inf.s2.lon1));
				System.out.println(String.format("Resolution and component flags (Table 7) = %d", this.dfs_grb1_inf.s2.flag));
				System.out.println(String.format("Orientation of the grid = %d", this.dfs_grb1_inf.s2.lov));
				System.out.println(String.format("X-dir. grid length = %d (Note 2)", this.dfs_grb1_inf.s2.dx));
				System.out.println(String.format("Y-dir. grid length = %d (Note 3)", this.dfs_grb1_inf.s2.dy));
			}
		}
		
	    //
	    //  Section 3 (BMS)
	    //
		if (((this.dfs_grb1_inf.s1.flag & 0xff) & 64) == 64) {
			if (dis.read(b, 0, 3) <= 0) {
				return -309;
			}

			this.dfs_grb1_inf.s3.length = (((b[0] & 0xff) * 256 + (b[1] & 0xff)) * 256) + (b[2] & 0xff);

			if (dis.read(b, 3, this.dfs_grb1_inf.s3.length - 3) <= 0) { 
				return -310;
			}
			
			if (DEBUG) {
				System.out.println("");
				System.out.println("[ Section 3 (BMS) ]");
				System.out.println(String.format("Section 3 length = %d bytes", this.dfs_grb1_inf.s3.length));
			}
		}

	    //
	    //  Section 4 (BDS)
	    //
		if (dis.read(b, 0, 3) <= 0) {
			return -311;
		}
		
		this.dfs_grb1_inf.s4.length = (((b[0] & 0xff) * 256 + (b[1] & 0xff)) * 256) + (b[2] & 0xff);

		if (dis.read(b, 3, 3) <= 0) {
			return -312;
		}

	    int b1 = b[3] & 0xff;
	    this.dfs_grb1_inf.s4.flag = (b1 >> 4) & 0x0f;
	    this.dfs_grb1_inf.s4.unused_bits = (b1 & 0x0f);

	    int sign = 1;
	    b1 = b[4] & 0xff;
	    if ((b1 & 0x80) > 0) sign = -1;
	    this.dfs_grb1_inf.s4.Scale_E = (((b[4] & 0xff) & 0x7f) * 256 + (b[5] & 0xff)) * sign;

		if (dis.read(b, 6, 4) <= 0) {
			return -312;
		}
		
	    if (((b[6] & 0xff) & 0x80) > 0) {
	        sign = -1;
	    } else {
	        sign = 1;
	    }

	    float refv_a = (b[6] & 0xff) & 0x7f;
	    float refv_b = ((b[7] & 0xff) * 256 + (b[8] & 0xff)) * 256 + (b[9] & 0xff);
	    this.dfs_grb1_inf.s4.minimum = (float) (sign * Math.pow(2.0, -24.0) * refv_b * Math.pow(16.0, refv_a - 64.0));

		if (dis.read(b, 10, 1) <= 0) {
			return -312;
		}
		
		this.dfs_grb1_inf.s4.num_bits = b[10] & 0xff;

		if (dis.read(buf, 0, this.dfs_grb1_inf.s4.length - 11) <= 0) {
			return -312;
		}

		if (DEBUG) {
		    System.out.println("");
		    System.out.println("[ Section 4 (BDS) ]");
		    System.out.println(String.format("Section 4 length = %d bytes", this.dfs_grb1_inf.s4.length));
		    System.out.println(String.format("flag = %d", this.dfs_grb1_inf.s4.flag));
		    System.out.println(String.format("unused_bits = %d", this.dfs_grb1_inf.s4.unused_bits));
		    System.out.println(String.format("Binary Scale Factor (E) = %d", this.dfs_grb1_inf.s4.Scale_E));
		    System.out.println(String.format("A = %.0f, B = %.0f", refv_a, refv_b));
		    System.out.println(String.format("Reference value (minimum value) = %f", this.dfs_grb1_inf.s4.minimum));
		    System.out.println(String.format("Number of bits = %d", this.dfs_grb1_inf.s4.num_bits));
		}
	    
	    //
	    //  Section 5 (END)
	    //
	    if (dis.read(b, 0, 4) <= 0) {
			return -313;
	    }
	    
		if (DEBUG) {
		    System.out.println("");
		    System.out.println("[ Section 5 (END) ]");
		    System.out.println(new String(b, 0, 4));
		}

		return this.dfs_grb1_inf.s4.length - 11;
	}

	private int bit_decode(final byte[] buf, int num_bits) {
		if (this.sbyte < 0 || this.sbit < 1 || this.sbit > 8 || num_bits < 0) {
			return -99999;
		}

		if (num_bits == 0) {
			return 0;
		}

		byte q;
	    int v = 0, v1 = 0, w1 = 0;
	    int[] s = { 0, 2, 4, 8, 16, 32, 64, 128, 256 };
		int[] m = { 0x00, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F, 0x7F, 0xFF };

	    while (num_bits > 0) {
	    	q = buf[this.sbyte];

	        v1 = q & m[this.sbit];

	        w1 = this.sbit - num_bits;
	        if (w1 <= 0) {
	            w1 = this.sbit;
	            v = v * s[w1] + v1;
	            num_bits -= w1;
	            this.sbyte += 1;
	            this.sbit = 8;
	        } else {
	            v1 = v1 >> w1;
	            v = v * s[num_bits] + v1;
	            this.sbit -= num_bits;
	            num_bits = 0;
	        }
	    }

	    return v;
	}

}
