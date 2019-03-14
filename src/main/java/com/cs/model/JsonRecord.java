package com.cs.model;

/**
 * @author paulf
 *
 */
public class JsonRecord {

	private String id;
	private String state;
	private String host;
	private String type;
	private long timestamp;	 

	/**
	 * @return the state
	 */
	public String getState() {
		if (state == null) {
			state = "";
		}
		return state;
	}

	/**
	 * @param set state
	 */
	public void setState(String state) {
		this.state = state;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param set id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		if (host == null) {
			host = "";
		}
		return host;
	}

	/**
	 * @param set host
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		if (type == null) {
			type = "";
		}
		return type;
	}

	/**
	 * @param set type
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param id the id to set
	 */
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "...type " + type + " , timestamp " + timestamp + ", state " + state + " , host " + host + " , id " + id
				+ " ";
	}
}
