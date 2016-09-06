String line = val.toString();
Polyline pline = null;
String temp = line.substring(line.indexOf("(")+1, line.lastIndexOf(")")); 

pline = WKTtoPolyline(temp);  

//¶¨ÒåWKTtoPolylineº¯Êý
private static Polyline WKTtoPolyline(String multipath)
{
      String subMultipath = multipath.substring(1, multipath.length()-1); 
      String[] paths; 
      paths = new String[]{subMultipath};  
      
      MultiPath path = null ;
      Point startPoint = null;
      path = new Polyline();
          String[] points = paths[0].split(",");  
          startPoint = null;
 for(int i=0;i<points.length;i++){                  
String[] pointStr = points[i].split(" ");  
if(startPoint == null){  
startPoint = new Point(Double.valueOf(pointStr[1]),Double.valueOf(pointStr[0]));  
path.startPath(startPoint);  
}
else
{                    
path.lineTo(new Point(Double.valueOf(pointStr[1]),Double.valueOf(pointStr[0])));  
 }                

       }
return path;
}