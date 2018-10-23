package cn.spark.study.core;

import java.io.Serializable;

import scala.math.Ordered;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
	private static final long serialVersionUID = 1L;
	
	private int first;
	private int second;
	
	public SecondarySortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	public int getFirst() {
		return first;
	}
	public void setFirst(int first) {
		this.first = first;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	@Override
	public boolean $greater(SecondarySortKey arg0) {
		if(this.getFirst() > arg0.getFirst()) {
			return true;
		} else if (this.getFirst() == arg0.getFirst() && this.getSecond() > arg0.getSecond()) {
			return true;
		}
		return false;
	}
	@Override
	public boolean $greater$eq(SecondarySortKey arg0) {
		if(this.$greater(arg0)) {
			return true;
		} else if(this.getFirst() == arg0.getFirst() && this.getSecond() == arg0.getSecond()) {
			return true;
		}
		return false;
	}
	@Override
	public boolean $less(SecondarySortKey arg0) {
		if(this.getFirst() < arg0.getFirst()) {
			return true;
		} else if (this.getFirst() == arg0.getFirst() && this.getSecond() < arg0.getSecond()) {
			return true;
		}
		return false;
	}
	@Override
	public boolean $less$eq(SecondarySortKey arg0) {
		if(this.$less(arg0)) {
			return true;
		} else if(this.getFirst() == arg0.getFirst() && this.getSecond() == arg0.getSecond()) {
			return true;
		}
		return false;
	}
	@Override
	public int compare(SecondarySortKey other) {
		if(this.first - other.getFirst() != 0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}
	@Override
	public int compareTo(SecondarySortKey other) {
		if(this.first - other.getFirst() != 0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
	}
}
