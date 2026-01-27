package kama.daemon.main.test;

import java.awt.*;
import java.awt.geom.*;
import java.awt.image.BufferedImage;

public class ArrowDrawerTest {
    
    /**
     * 화살표를 그리는 메서드 (몸통과 헤드를 모두 포함)
     * @param g2d Graphics2D 객체
     * @param startX 시작점 X 좌표
     * @param startY 시작점 Y 좌표
     * @param length 화살표 길이
     * @param angle 화살표 각도 (라디안)
     * @param thickness 화살표 두께
     * @param headSize 헤드 크기
     * @param color 화살표 색상
     */
    public static void drawArrow(Graphics2D g2d, double startX, double startY, 
                               double length, double angle, double thickness, 
                               double headSize, Color color) {
        
        // 안티앨리어싱 설정
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        
        // 색상 설정
        g2d.setColor(color);
        
        // 끝점 계산
        double endX = startX + length * Math.cos(angle);
        double endY = startY + length * Math.sin(angle);
        
        // 화살표 몸통 그리기
        // 화살표 방향에 수직인 벡터 계산
        double dx = endX - startX;
        double dy = endY - startY;
        double arrowLength = Math.sqrt(dx * dx + dy * dy);
        
        if (arrowLength > 0) {
            // 단위 벡터 계산
            double unitX = dx / arrowLength;
            double unitY = dy / arrowLength;
            
            // 수직 벡터 계산
            double perpX = -unitY;
            double perpY = unitX;
            
            // 두께의 절반
            double halfThickness = thickness / 2.0;
            
            // 사각형의 네 꼭지점 계산
            double[] xPoints = {
                startX + perpX * halfThickness,
                startX - perpX * halfThickness,
                endX - perpX * halfThickness,
                endX + perpX * halfThickness
            };
            
            double[] yPoints = {
                startY + perpY * halfThickness,
                startY - perpY * halfThickness,
                endY - perpY * halfThickness,
                endY + perpY * halfThickness
            };
            
            // 사각형 그리기 (몸통)
            g2d.fill(new Polygon(
                new int[]{(int)xPoints[0], (int)xPoints[1], (int)xPoints[2], (int)xPoints[3]},
                new int[]{(int)yPoints[0], (int)yPoints[1], (int)yPoints[2], (int)yPoints[3]},
                4
            ));
        }
        
        // 화살표 기준점 계산
        double arrowX = startX + length * 0.9 * Math.cos(angle);
        double arrowY = startY + length * 0.9 * Math.sin(angle);
        
        // 화살표 헤드 그리기 (삼각형 모양으로 변경)
        // 헤드의 각도 (라디안)
        double headAngle = Math.PI / 2; // 30도
        
        // 헤드의 두 점 계산
        double headAngle1 = angle + headAngle;
        double headAngle2 = angle - headAngle;
        
        // 헤드의 두 점 좌표
        double headX1 = arrowX - headSize * Math.cos(headAngle1);
        double headY1 = arrowY - headSize * Math.sin(headAngle1);
        double headX2 = arrowX - headSize * Math.cos(headAngle2);
        double headY2 = arrowY - headSize * Math.sin(headAngle2);
        
        double headEndX = arrowX + headSize * 2 * Math.cos(angle);
        double headEndY = arrowY + headSize * 2 * Math.sin(angle);    
        
        double headEndX2 = arrowX + headSize / 2 * Math.cos(angle);
        double headEndY2 = arrowY + headSize / 2 * Math.sin(angle);
        
        // 헤드좌표 전체를 화살표 몸통 각도의 반대로 옴겨야함
        // 헤드 끝점 좌표
        
        
        // 헤드 그리기 (삼각형)
        int[] xPoints = {(int)headEndX, (int)headX1, (int)headEndX2, (int)headX2};
        int[] yPoints = {(int)headEndY, (int)headY1, (int)headEndY2, (int)headY2};
        
        g2d.fill(new Polygon(xPoints, yPoints, 4));
        
        // 디버깅: 헤드 좌표 출력
        System.out.println("헤드 좌표:");
        System.out.println("끝점: (" + (int)endX + ", " + (int)endY + ")");
        System.out.println("헤드1: (" + (int)headX1 + ", " + (int)headY1 + ")");
        System.out.println("헤드2: (" + (int)headX2 + ", " + (int)headY2 + ")");
    }
    
    /**
     * 예제 사용법
     */
    public static void main(String[] args) {
        // 이미지 생성
        int width = 400;
        int height = 300;
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2d = image.createGraphics();
        
        // 배경을 흰색으로 설정
        g2d.setColor(Color.WHITE);
        g2d.fillRect(0, 0, width, height);
        
        // 화살표 그리기 (왼쪽 상단에서 오른쪽 하단으로)
        double startX = 50;
        double startY = 50;
        double length = 200;
        double angle = Math.PI / 4.0; // 45도
        double thickness = 2.0;
        double headSize = 10.0;
        Color arrowColor = Color.BLACK;
        
        drawArrow(g2d, startX, startY, length, angle, thickness, headSize, arrowColor);
        
        g2d.dispose();
        
        // 이미지를 파일로 저장 (선택사항)
        try {
            javax.imageio.ImageIO.write(image, "PNG", new java.io.File("F:/data/arrow.png"));
            System.out.println("화살표 이미지가 'arrow.png'로 저장되었습니다.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}