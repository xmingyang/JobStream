package org.jobstream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class JobStream {
	//项目代码，项目标识
	public String getProject_en() {
		return project_en;
	}




	public void setProject_en(String project_en) {
		this.project_en = project_en;
	}



 //crontab id，只有被自动调度的时候才有该id,写日志带入，手工跑该类id=0
	public int getCrontab_id() {
		return crontab_id;
	}




	public void setCrontab_id(int crontab_id) {
		this.crontab_id = crontab_id;
	}



//整个项目执行统一的序号，基于当前时间生成，作为版本号使用
	public String getScheduler_seq() {
		return scheduler_seq;
	}




	public void setScheduler_seq(String scheduler_seq) {
		this.scheduler_seq = scheduler_seq;
	}




	private String project_en;
	private int crontab_id=0;
	private String scheduler_seq;
	private int max=2; //并发数的控制，默认值2
	//不否要断点自能重跑
	private String is_last_rerun="";
	public String getIs_last_rerun() {
		return is_last_rerun;
	}




	public void setIs_last_rerun(String is_last_rerun) {
		this.is_last_rerun = is_last_rerun;
	}




	public String getLast_scheduler_seq() {
		return last_scheduler_seq;
	}




	public void setLast_scheduler_seq(String last_scheduler_seq) {
		this.last_scheduler_seq = last_scheduler_seq;
	}



   //断点自动重跑的版本号
	private String last_scheduler_seq="";
	
	public int getMax() {
		return max;
	}




	public void setMax(int max) {
		this.max = max;
	}

	//手工指定跑批起点job
	 private ArrayList<String> joblist=new ArrayList<String>();
	 
   


	public ArrayList<String> getJoblist() {
		return joblist;
	}




	public void setJoblist(ArrayList<String> joblist) {
		this.joblist = joblist;
	}
	//是否以起点job构建后续依赖关系
	private String is_dag="true";
	




	public String getIs_dag() {
		return is_dag;
	}




	public void setIs_dag(String is_dag) {
		this.is_dag = is_dag;
	}




	ArrayList<JobInfo> jobqueue;
//	Map<String,String> outputjob; //每个输出由哪个job产生
	Map<String,ArrayList<String>> outputjob;
	Map<String,String> stautsmap;//job运行状态
	Map<String,JobInfo> jobinfomap;//job基本信息
	Map<String,ArrayList<String>> inputjobs; //每个input都有哪些job依赖
	Map<String,String> project_param;
	Map<String,String> runningmap;
	
	public JobStream(ArrayList<JobInfo> jobqueue,Map<String,ArrayList<String>> outputjob,Map<String,String> stautsmap,Map<String,JobInfo> jobinfomap,Map<String,ArrayList<String>> inputjobs,Map<String,String> project_param,Map<String,String> runningmap )
	{
		this.jobqueue=jobqueue;
		this.outputjob=outputjob;
		this.stautsmap=stautsmap;
		this.jobinfomap=jobinfomap;
		this.inputjobs=inputjobs;
		this.project_param=project_param;
		this.runningmap=runningmap;
	}


	public void init()
	{
		
		PropertyConfigurator.configure("conf/log4j.properties");
		Logger logger = Logger.getLogger(JobStream.class.getName());
		logger.info("init start!");
		Connection con = null;
		Statement sql = null;
		ResultSet rs = null;

		try {
			con = DbCoonect.getConnectionMySql();
			if (con == null) {
				logger.error("connect is null");
				System.exit(0);
			}
			sql = con.createStatement();
			
			String strSql = "select trim(project_en),trim(project_cn),trim(job_en),trim(job_cn),priority,trim(ip),port,trim(user),trim(path),trim(hdfs_input),trim(hdfs_output),trim(c.job_type),trim(a.param) from proj_jobdetail a,project b,proj_jobtype c where "
					+ " a.project_id=b.id and a.job_type_id=c.id and trim(b.project_en)= '"+project_en+"'";
			
			//不构建后续依赖关系。
			if (joblist.size()>0 && getIs_dag().equals("false"))
			{
				
				strSql="select trim(project_en),trim(project_cn),trim(job_en),trim(job_cn),priority,trim(ip),port,trim(user),trim(path),trim(hdfs_input),trim(hdfs_output),trim(c.job_type),trim(a.param) from proj_jobdetail a,project b,proj_jobtype c where"
						+ " a.project_id=b.id and a.job_type_id=c.id and trim(b.project_en)= '"+project_en+"' " +
						"and ( trim(job_en)='";
				for (int i=0;i<joblist.size();i++)
				{
					if (i==0)
						strSql=strSql+joblist.get(i)+"'";
					else
						
						strSql=strSql+" or trim(job_en)='"+joblist.get(i)+"'";
					
				}
				strSql=strSql+")";
			}
			
			
			String projectSql="select trim(param),max from project where trim(project_en)='"+project_en+"'";
	       rs=sql.executeQuery(projectSql);
	       //项目级别参数
	      if (rs.next())
	      {
	    	  //项目参数
	    	  String param=rs.getString(1);
	    	  //项目并发个数
	    	  int maxval=rs.getInt(2);
	    	  if (maxval>0)
	    	  {
	    		  setMax(maxval);
	    	  }
	    	  if ( param!=null)
	    	  {
	    		  if (!param.equals("") )
	    		  {
	    	  String[] params=param.split(";");
	    	   for (int i=0;i<params.length;i++)
	    	   {
	    		   String[] param_kv=params[i].split("=");
	    		   project_param.put(param_kv[0],CommonUtil.expr_date(param_kv[1]));
	    	   }
	    		  }
	    	  }
	    	
	      }
			//如果指定了断点重跑
			if (getIs_last_rerun().equals("true"))
			{
				//并且指定了版本号
				if (!getLast_scheduler_seq().equals(""))
				{
					strSql = "select trim(project_en),trim(project_cn),trim(job_en),trim(job_cn),priority,trim(ip),port,trim(user),trim(path),trim(hdfs_input),trim(hdfs_output),trim(c.job_type),trim(a.param) from proj_jobdetail a,project b,proj_jobtype c where"
							+ "  a.project_id=b.id and a.job_type_id=c.id and trim(b.project_en)= '"+project_en+"' and not exists " +
							"( select 1 from proj_log bb where trim(b.project_en)=trim(bb.project_en) and trim(a.job_en)=trim(bb.job_en) and bb.proj_scheduler_seq='"+getLast_scheduler_seq()+"' and bb.datekey='" +new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
							+"' and bb.program_status='"+"C"+ "')";
				}
				//没指定版本号，则使用当天最大的一个版本号
				else
				{
					rs=sql.executeQuery("select max(proj_scheduler_seq) from proj_log  where project_en='"+project_en+"' and datekey='" +new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
							+ "'");
					if (rs.next())
					{
						String max_scheduler_seq=rs.getString(1);
				//		System.out.println("max_scheduler_seq:"+max_scheduler_seq);
						strSql = "select trim(project_en),trim(project_cn),trim(job_en),trim(job_cn),priority,trim(ip),port,trim(user),trim(path),trim(hdfs_input),trim(hdfs_output),trim(c.job_type),trim(a.param) from proj_jobdetail a,project b,proj_jobtype c where"
								+ " a.project_id=b.id and a.job_type_id=c.id and trim(b.project_en)= '"+project_en+"' and not exists " +
								"( select 1 from proj_log bb where trim(b.project_en)=trim(bb.project_en) and trim(a.job_en)=trim(bb.job_en) and bb.proj_scheduler_seq='"+max_scheduler_seq+"' and bb.datekey='" +new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
								+"' and bb.program_status='"+"C"+ "')";
						
					}
					
				}
				
			}
			

			rs = sql.executeQuery(strSql);
			while (rs.next()) {
				
				String job_en=rs.getString(3);
              JobInfo jobinfo=new JobInfo(job_en);
              jobinfo.setJob_cn(rs.getString(4));
              jobinfo.setPriority(rs.getInt(5));
              jobinfo.setIp(rs.getString(6));
              jobinfo.setPort(rs.getInt(7));
              jobinfo.setUser(rs.getString(8));
              jobinfo.setPath(rs.getString(9));
              jobinfo.setJob_type(rs.getString(12));
             jobinfo.setParam(rs.getString(13));
              String  hdfs_input=rs.getString(10);
              if ( hdfs_input!=null)
              {
            	if (!hdfs_input.trim().equals(""))
            	{
              String[] inputs=hdfs_input.split(";");
              ArrayList<String> inputs1=new ArrayList<String>();
              Collections.addAll(inputs1, inputs);
              jobinfo.setInputs(inputs1);
              
              for (String input : inputs)
              {
            	  if (inputjobs.containsKey(input.trim()))
            	  {
            		  inputjobs.get(input.trim()).add(job_en);
            	  }
            	  else 
            	  {
            		  ArrayList<String> jobs=new ArrayList<String>() ;
            		  jobs.add(job_en);
            		  inputjobs.put(input.trim(), jobs);
            		  
            	  }
              }
            	}
              }
              
             
            	  
              String hdfs_ouput=rs.getString(11);
              if ( hdfs_ouput!=null)
              {
            	  if (!hdfs_ouput.trim().equals(""))
              	{
              String[] outputs=hdfs_ouput.split(";");
              ArrayList<String> outputs1=new ArrayList<String>();
              Collections.addAll(outputs1, outputs);
              jobinfo.setOutputs(outputs1);
              for (String output : outputs)
              {
            	  if (outputjob.containsKey(output.trim()))
            	  {
            		  outputjob.get(output.trim()).add(job_en);
            	  }
            	  else 
            	  {
            		  ArrayList<String> jobs1=new ArrayList<String>() ;
            		  jobs1.add(job_en);
            		  outputjob.put(output.trim(), jobs1);
            		  
            	  }
            	  
            	  
              }
              	}
              }
              jobinfomap.put(job_en, jobinfo);
              
			}
		
			
			
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error(e.getMessage());
			

		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				//e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
		
		//遍历所有的作业，构建依赖关系
		
	    for(Map.Entry<String , JobInfo> entry : jobinfomap.entrySet()){  
	    	JobInfo jinfo=entry.getValue();
	    	ArrayList<String> jinfo_inputs=jinfo.getInputs();
	    	//依赖于哪些作业
	    	for (String jinput:jinfo_inputs)
	    	{
	    		if (outputjob.containsKey(jinput.trim()))
	    		{
	    			ArrayList<String> outals=outputjob.get(jinput.trim());
	    			for (String outals_item:outals)
	    			{
	    				jinfo.getRefjobs().add(outals_item);
	    			}
	    		}
	    	//	String	jinfo_refitem=outputjob.get(jinput.trim());
	    	//	if (jinfo_refitem!=null && !jinfo_refitem.equals(""))
	    	//	jinfo.getRefjobs().add(jinfo_refitem);
	    	}
	    	//被哪些作业依赖
	    	ArrayList<String> jinfo_ouputs=jinfo.getOutputs();
	    	
	    	for (String joutput:jinfo_ouputs)
	    	{
	    		if (inputjobs.containsKey(joutput.trim()))
	    		{
	    		ArrayList<String> als=inputjobs.get(joutput.trim());
	    		for (String als_item:als)
	    		{
	    			jinfo.getRefedjobs().add(als_item);
	    		}
	    		}
	    	
	    		
	    	}
	    	
	    	logger.info(jinfo.getJob_en()+" ref jobs:"+jinfo.getRefjobs().toString());
	    	logger.info(jinfo.getJob_en()+" refed jobs:"+jinfo.getRefedjobs().toString());
	    	//无任何依赖的作业加入队列
	   
	    	if(joblist.size()==0 || getIs_dag().equals("false"))
	    	{
	    	if (jinfo.getRefjobs().size()==0)
	    		{
	    		long starttime=new Date().getTime();
	    		jinfo.setStarttime(starttime);
	    		jobqueue.add(jinfo);   		
	    		}
	    	logger.info("init jobqueue size:"+jobqueue.size());
	    	}
	       
	     }
	    //以起点作业构建dag
	    //有些冗余可优化
	    if (joblist.size()>0 && getIs_dag().equals("true"))
	    {
	    	
	    	
	    	HashSet<String> hs=new HashSet<String>();
	    	//所有需要待跑的作业
	    	HashSet<String> hsall=new HashSet<String>();
	    	for (String itemlist:joblist)
	    	{
	    		hsall.add(itemlist);
	    		HashSet<String> hs0=jobinfomap.get(itemlist).getRefedjobs();
	    		for (String hs0_item:hs0)
	    		{
	    			hs.add(hs0_item);
	    			hsall.add(hs0_item);
	    		}
	    		
	    	}
	    	
	    	
	    	HashSet<String> hs1=null;
	    	while (true)
	    	{
	    		if (hs.size()==0)
	    		 break;
	    		hs1=new HashSet<String>();
	    		for (String hs_item:hs)
	    		{
	    			HashSet<String> hs0=jobinfomap.get(hs_item).getRefedjobs();
		    		for (String hs0_item:hs0)
		    		{
		    			hs1.add(hs0_item);
		    			hsall.add(hs0_item);
		    		}
	    		}
	    		hs=hs1;
	    	}
	    	
	    	
	    	
	    	//hashset转hashmap,为了便于查找
	    	HashMap<String,String> hsall_map=new HashMap<String,String>();
	    	for(String hsall_item:hsall)
	    	{
	    		hsall_map.put(hsall_item, hsall_item);
	    	}
	    	hsall=null;
	    	
	    	//把不在hsall里的作业从jobinfomap里移除，包括每个作业的ref refedjobs
	    	HashMap<String,JobInfo> jobinfomap1=new HashMap<String,JobInfo> (jobinfomap);
	    	for(Map.Entry<String , JobInfo> entry : jobinfomap1.entrySet())
	    	{ 
	    		JobInfo jinfo=jobinfomap.get(entry.getKey());
	    		if (hsall_map.containsKey(entry.getKey()))
	    		{
	    			HashSet<String> refjobs=jinfo.getRefjobs();
	    			HashSet<String> refjobs1=new HashSet<String>(refjobs);
	    			HashSet<String> refedjobs=jinfo.getRefedjobs();
	    			HashSet<String> refedjobs1=new HashSet<String>(refedjobs);
	    			Iterator<String>  refjobs_iter= refjobs1.iterator();
	    			Iterator<String>  refedjobs_iter= refedjobs1.iterator();
	    			while(refjobs_iter.hasNext())
	    			{
	    				String ref_item=refjobs_iter.next();
	    				if (!hsall_map.containsKey(ref_item))
	    				{
	    					refjobs.remove(ref_item);
	    				}
	    				
	    			}
	    			refjobs1=null;
	    			//此去可去掉
	    			while(refedjobs_iter.hasNext())
	    			{
	    				String refed_item=refedjobs_iter.next();
	    				if (!hsall_map.containsKey(refed_item))
	    				{
	    					refedjobs.remove(refed_item);
	    				}
	    				
	    			}
	    			refedjobs1=null;
	    		
	    			if (jinfo.getRefjobs().size()==0)
		    		{
		    		long starttime=new Date().getTime();
		    		jinfo.setStarttime(starttime);
		    		jobqueue.add(jinfo);   		
		    		}
		    	logger.info("init jobqueue size:"+jobqueue.size());
	    			
	    		}
	    		
	    		
	    		
	    		else
	    			{
	    			jobinfomap.remove(entry.getKey());
	    			}
	    		
	    		
	    	}
	    	jobinfomap1=null;
	    	
	    	
	    	
	    }
	    
	    
	    resortJobqueue();
	    
	}
	
	 public  void resortJobqueue()
	 {
		 Comparator<JobInfo> comp = new Comparator<JobInfo>() {
		      public int compare(JobInfo o1, JobInfo o2) {
		        int res = o1.getPriority()-o2.getPriority();
		        if(res == 0) {
		          if(o1.getStarttime() < o2.getStarttime())
		            res = 1;
		          
		          else
		            res = (o1.getStarttime()==o2.getStarttime() ? 0 : -1);
		        }
		          
		        return -res;
		      }
		    };
		    
		    synchronized (jobqueue) {
		      Collections.sort(jobqueue, comp);
		    }
	 }
	
	
	public void start() 
	{ 
		PropertyConfigurator.configure("conf/log4j.properties");
		Logger logger = Logger.getLogger(JobStream.class.getName());
	  int x=0;
	  int c=0; //状态为s的个数
		while (true)
		{
	//	logger.info("project_en:"+this.getProject_en()+" crontab_id:"+this.getCrontab_id()+" while true");	
			
			synchronized (jobqueue) {
			if (jobqueue.isEmpty())
			{ 
				if (runningmap.size()==0)
				{
					
					x++;
					logger.info("project_en:"+this.getProject_en()+" crontab_id:"+this.getCrontab_id()+" jobqueue is empty and  running jobs:0:seq:"+x);
				}
				try {
					jobqueue.wait(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			}
		//	 System.out.println("start jobqueue size:"+jobqueue.size()+" jobqueue string:"+jobqueue.toString());
		//	c=JobRunner.scnt;  
			 c=runningmap.size();
			 if (c<=getMax() )  
			 {
		//		logger.info("project_en:"+this.getProject_en()+" crontab_id:"+this.getCrontab_id()+" less than max and running jobs:"+c);
				 JobInfo jobinfo=null;
				 if (!jobqueue.isEmpty())
				 {
				  jobinfo=jobqueue.remove(0);				 
				 }
			 
			if (jobinfo!=null)
				
				{	
				//logger.info("c<getMax:c:"+c+" x:0");
				x=0;
				JobRunner jr=new JobRunner(jobinfo,jobqueue,stautsmap, project_en, crontab_id, scheduler_seq,jobinfomap,project_param,runningmap);
				jr.start();
				try {
					Thread.sleep(600);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				}
			 }
			 else 
			 {
			//	logger.info("project_en:"+this.getProject_en()+" crontab_id:"+this.getCrontab_id()+" no less than max and running jobs:"+c);
			//	 logger.info("c>=getMax:c:"+c+" x:0");
				 x=0;
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			 }
			
			if (x>20)
			{
			logger.info("project_en:"+this.getProject_en()+" crontab_id:"+this.getCrontab_id()+" the whole JobStream  job exit!");
			break;
			}
			
		}
	}
	
	public static void main(String[] args)
	{
		/*
		 * 传参：
		 * 必需：
		 * project_en 项目名
		 * 可选 :is_last_rerun 是否断点重跑
		 * last_scheduler_seq 断点重跑的版本号
		 * 
		 * 
		 */
		if (args.length<1)
		{
			System.out.println("必须传入参数");
			System.exit(1);
		}
		HashMap<String,String> param_value=new HashMap<String,String>();
		for (String para:args)
		{
			if (!para.contains("="))
			{
				System.out.println("传入参数格式:param_name=param_value");
				System.exit(1);
			}
			String[] s=para.split("=");
			param_value.put(s[0], s[1]);
			
			
			
		}
		if(!param_value.containsKey("project_en"))
		{
			System.out.println("必须传入参数:project_en");
			System.exit(1);
		}
		/*
		if(!param_value.containsKey("max"))
		{
			System.out.println("必须传入参数:max");
			System.exit(1);
		}
		*/
		ArrayList<JobInfo> jobqueue =new ArrayList<JobInfo>();
		Map<String,ArrayList<String>> outputjob=new HashMap<String,ArrayList<String>>(); //每个输出由哪个job产生
		Map<String,String> stautsmap=new ConcurrentHashMap<String,String>();//job运行状态
		Map<String,JobInfo> jobinfomap=new  HashMap<String,JobInfo>();//job基本信息
		Map<String,ArrayList<String>> inputjobs=new HashMap<String,ArrayList<String>>(); //每个input都有哪些job依赖
		Map<String,String> project_param=new HashMap<String,String>();
		Map<String,String> runningmap =new ConcurrentHashMap<String,String>();
		JobStream jobmain=new JobStream(jobqueue,outputjob,stautsmap,jobinfomap,inputjobs,project_param,runningmap);
		jobmain.setProject_en(param_value.get("project_en"));
		jobmain.setScheduler_seq(new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()));
		if(param_value.containsKey("is_last_rerun"))
		{
			//如果是断点重跑，必须指定最大并发数max,若last_scheduler_seq没有指定，则默认取当天最大的一个版本
			if (param_value.get("is_last_rerun").equals("true"))
			{
				jobmain.setIs_last_rerun("true");
				if (param_value.containsKey("last_scheduler_seq"))
					jobmain.setLast_scheduler_seq(param_value.get("last_scheduler_seq"));
				
			}
		}
		if (param_value.containsKey("joblist"))
		{
			
			String[] jobs=param_value.get("joblist").split(",");
			ArrayList<String> jobs1=new ArrayList<String>();
			Collections.addAll(jobs1, jobs);
			jobmain.setJoblist(jobs1);
			
		   if (param_value.containsKey("is_dag"))
		   {
			   jobmain.setIs_dag(param_value.get("is_dag"));
		   }
			
			
		}
	//	jobmain.setMax(Integer.parseInt(param_value.get("max")));
		jobmain.init();
		jobmain.start();
	}
	
	

}
