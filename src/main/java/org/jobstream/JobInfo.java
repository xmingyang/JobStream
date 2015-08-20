package org.jobstream;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

public class JobInfo {
	
	private int priority;

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}

	private long starttime;
	private String job_en;
	private String job_cn;

	public String getJob_en() {
		return job_en;
	}

	public void setJob_en(String job_en) {
		this.job_en = job_en;
		
	}

	public String getJob_cn() {
		return job_cn;
	}

	public void setJob_cn(String job_cn) {
		this.job_cn = job_cn;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	private String ip;
	private int port;
	private String user;
	private String path;
	private String job_type;
	private String param="";
	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}

	public String getJob_type() {
		return job_type;
	}

	public void setJob_type(String job_type) {
		this.job_type = job_type;
	}

	// job的所有输入路径
	private ArrayList<String> inputs=new ArrayList<String>();
	private ArrayList<String> outputs=new ArrayList<String>();

	public ArrayList<String> getOutputs() {
		return outputs;
	}

	public void setOutputs(ArrayList<String> outputs) {
		this.outputs = outputs;
	}

	public ArrayList<String> getInputs() {
		return inputs;
	}

	public void setInputs(ArrayList<String> inputs) {
		this.inputs = inputs;
	}

	private HashSet<String> refjobs=new HashSet<String>(); // job依赖哪些job

	public HashSet<String> getRefjobs() {
		return refjobs;
	}

	public void setRefjobs(HashSet<String> refjobs) {
		this.refjobs = refjobs;
	}

	public HashSet<String> getRefedjobs() {
		return refedjobs;
	}

	public void setRefedjobs(HashSet<String> refedjobs) {
		this.refedjobs = refedjobs;
	}

	private HashSet<String> refedjobs=new HashSet<String>(); // job被哪些job依赖

	public JobInfo(String job_en) {

		this.job_en = job_en;
	}

	public int getPriority() {
		return priority;

	}

	public long getStarttime() {
		return starttime;
	}

	public String toString() {
		return job_en;
	}
}
