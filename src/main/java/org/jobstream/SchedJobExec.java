package org.jobstream;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class SchedJobExec implements Job {
	public void execute(JobExecutionContext jobCtx)throws JobExecutionException {
		PropertyConfigurator.configure("conf/log4j.properties");
		Logger logger = Logger.getLogger(SchedJobExec.class.getName());
		

	//	System.out.println( " triggered. time is:" + (new Date()));
		JobDataMap data = jobCtx.getJobDetail().getJobDataMap(); 
		String project_en=data.getString("project_en");
		int crontab_id=data.getInt("crontab_id");
		String param=data.getString("param");
	//	int max=data.getInt("max");
		logger.info("project exec:"+"project_en:"+project_en+" crontab_id:"+crontab_id+" param:"+param);
		ArrayList<JobInfo> jobqueue =new ArrayList<JobInfo>();
		Map<String,ArrayList<String>> outputjob=new HashMap<String,ArrayList<String>>(); //每个输出由哪个job产生
		Map<String,String> stautsmap=new ConcurrentHashMap<String,String>();//job运行状态
		Map<String,JobInfo> jobinfomap=new HashMap<String,JobInfo>();//job基本信息
		Map<String,ArrayList<String>> inputjobs=new HashMap<String,ArrayList<String>>(); //每个input都有哪些job依赖
		Map<String,String> project_param=new HashMap<String,String>();
		Map<String,String> runningmap =new ConcurrentHashMap<String,String>();
		Map<String,String> pre_runningmap =new HashMap<String,String>();
		//设置了after time的作业，正运行还未到达after time时间的作业map列表
				Map<String,String> runningmap_aftertime =new ConcurrentHashMap<String,String>();
		JobStream jobmain=new JobStream(jobqueue,outputjob,stautsmap,jobinfomap,inputjobs,project_param,runningmap,pre_runningmap,runningmap_aftertime);
		jobmain.setProject_en(project_en);
		jobmain.setScheduler_seq(new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()));
		jobmain.setCrontab_id(crontab_id);
		jobmain.setCrontab_param(param);
	//	jobmain.setMax(max);
		jobmain.init();
		jobmain.start();
		


		}
	

}
