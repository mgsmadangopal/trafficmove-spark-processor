package com.trafficmove.app.spark.vo;

import java.io.Serializable;

/**
 * Class to represent attributes of POI
 */
public class POIData implements Serializable {
	private double latitude;
	private double longitude;
	private double radius;
	
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getRadius() {
		return radius;
	}
	public void setRadius(double radius) {
		this.radius = radius;
	}
}
