package kama.daemon.common.util;

public class KmaDfsConverter {

    // --- DFS (동네예보) 상수 ---
    private static final double RE   = 6371.00877; // 지구 반경(km)
    private static final double GRID = 5.0;        // 격자 간격(km)
    private static final double SLAT1 = 30.0;      // 표준위도1(도)
    private static final double SLAT2 = 60.0;      // 표준위도2(도)
    private static final double OLON  = 126.0;     // 기준경도(도)
    private static final double OLAT  = 38.0;      // 기준위도(도)
    private static final double XO    = 43.0;      // 기준점 X 좌표
    private static final double YO    = 136.0;     // 기준점 Y 좌표

    private static final double DEGRAD = Math.PI / 180.0;
    private static final double RADDEG = 180.0 / Math.PI;

    // 내부에서 재사용하는 투영 계수
    private static class Coeff {
        final double re, sn, sf, ro, olon, olat;
        Coeff(double re, double sn, double sf, double ro, double olon, double olat) {
            this.re = re; this.sn = sn; this.sf = sf; this.ro = ro; this.olon = olon; this.olat = olat;
        }
    }

    private static Coeff coeff() {
        double re = RE / GRID;
        double slat1 = SLAT1 * DEGRAD;
        double slat2 = SLAT2 * DEGRAD;
        double olon  = OLON  * DEGRAD;
        double olat  = OLAT  * DEGRAD;

        double sn = Math.tan(Math.PI * 0.25 + slat2 * 0.5) / Math.tan(Math.PI * 0.25 + slat1 * 0.5);
        sn = Math.log(Math.cos(slat1) / Math.cos(slat2)) / Math.log(sn);

        double sf = Math.tan(Math.PI * 0.25 + slat1 * 0.5);
        sf = Math.pow(sf, sn) * (Math.cos(slat1) / sn);

        double ro = Math.tan(Math.PI * 0.25 + olat * 0.5);
        ro = re * sf / Math.pow(ro, sn);

        return new Coeff(re, sn, sf, ro, olon, olat);
    }

    // 위경도 -> 격자 (정수 X,Y와 소수 포함 X,Y 모두 반환)
    public static DfsGrid latLonToDfsGrid(double lat, double lon) {
        Coeff c = coeff();

        double ra = Math.tan(Math.PI * 0.25 + (lat * DEGRAD) * 0.5);
        ra = c.re * c.sf / Math.pow(ra, c.sn);

        double theta = lon * DEGRAD - c.olon;
        if (theta >  Math.PI) theta -= 2.0 * Math.PI;
        if (theta < -Math.PI) theta += 2.0 * Math.PI;
        theta *= c.sn;

        double x = ra * Math.sin(theta) + XO + 0.5;         // +0.5는 기상청 권장(반올림 유도)
        double y = c.ro - ra * Math.cos(theta) + YO + 0.5;

        return new DfsGrid((int)Math.floor(x), (int)Math.floor(y), x, y);
    }

    // 격자 -> 위경도 (격자 X,Y를 넣으면 위도/경도 반환)
    public static DfsLatLon gridToDfsLatLon(int x, int y) {
        Coeff c = coeff();

        double xn = x - XO;
        double yn = c.ro - y + YO;

        double ra = Math.hypot(xn, yn);
        if (c.sn < 0.0) ra = -ra;

        double alat = Math.pow(c.re * c.sf / ra, 1.0 / c.sn);
        alat = 2.0 * Math.atan(alat) - Math.PI * 0.5;

        double theta;
        if (Math.abs(xn) <= 1e-7 && Math.abs(yn) <= 1e-7) {
            theta = 0.0;
        } else {
            theta = Math.atan2(xn, yn);
        }

        double alon = theta / c.sn + c.olon;

        return new DfsLatLon(alat * RADDEG, alon * RADDEG);
    }

    // 결과 담는 간단한 DTO들
    public static class DfsGrid {
        public final int nx, ny;           // 반올림된 정수 격자
        public final double x, y;          // 소수 포함 원값(디버그/검증용)
        public DfsGrid(int nx, int ny, double x, double y) {
            this.nx = nx; this.ny = ny; this.x = x; this.y = y;
        }
        @Override public String toString() {
            return String.format("DfsGrid(nx=%d, ny=%d, x=%.6f, y=%.6f)", nx, ny, x, y);
        }
    }

    public static class DfsLatLon {
        public final double lat, lon;
        public DfsLatLon(double lat, double lon) { this.lat = lat; this.lon = lon; }
        @Override public String toString() {
            return String.format("DfsLatLon(lat=%.6f, lon=%.6f)", lat, lon);
        }
    }

    // 사용 예시
    public static void main(String[] args) {
        // 예: 서울시청 근처 (위도 37.5665, 경도 126.9780)
    	
    	//33.5082472222222	126.513733333333
    	
        double lat = 35.59;
        double lon = 129.36;

        DfsGrid g = latLonToDfsGrid(lat, lon);
        System.out.println("위경도 -> 격자: " + g); // 보통 nx=60, ny=127 나옵니다.

        DfsLatLon ll = gridToDfsLatLon(g.nx, g.ny);
        System.out.println("격자 -> 위경도: " + ll);
    }
}
