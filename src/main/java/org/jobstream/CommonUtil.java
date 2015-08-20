package org.jobstream;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CommonUtil {
	public static void sendmail(String msg,String title)
	{
		 PropertyConfigurator.configure("conf/log4j.properties");
			Logger logger = Logger.getLogger(CommonUtil.class.getName());
			logger.info("调用本地shell发邮件，邮件内容为:"+msg);
	        //String newmsg=msg.replace(" ", "");
			
	
		  try { 
			  String cmd=PropHelper.getStringValue("cmd");
				String emaillist=PropHelper.getStringValue("email");
			//	logger.info("email command:"+cmd+" \'"+newmsg+"\' "+title+" "+emaillist);
				
				String[] cmds = new String[4];
				cmds[0] = cmd;
			    cmds[1]=msg;
			    cmds[2]=title;
			    cmds[3]=emaillist;
			    logger.info("email command:"+cmds);
	
	   //   Process process= Runtime.getRuntime().exec(cmd+" \""+newmsg+"\" "+title+" "+emaillist); 
			    Process process= Runtime.getRuntime().exec(cmds); 
	      int exitValue = process.waitFor(); 
	      if (0 != exitValue) {
		  logger.error("邮件发送失败. error code is :" + exitValue); 
		  }
	      else
	      {
	    	  logger.info("邮件发送成功.emailist:"+emaillist+" title:"+title);
	      }
	      }
		  
		  catch
		  (Exception e)
		  { logger.error("邮件发送失败. " + e); }
	}
	public static String expr_date(String expr_date)
	{
		Pattern pat = Pattern.compile("expr_date\\(([date|hour].*),([a-zA-z0-9-/ ]+)\\)");  
		Matcher mat = pat.matcher(expr_date); 
		if (mat.find())
		{
			String dateval=mat.group(1);
			String dateformat=mat.group(2);
			
			Calendar thiscal=Calendar.getInstance();
			  
			  Date   thisb=new Date();
			 thiscal.setTime(thisb);
			 
				  
				 // System.out.println(sdf.format(thiscal.getTime()));
			 SimpleDateFormat sdf=new SimpleDateFormat(dateformat);
				  
			if (dateval.contains("-"))
			{
				String[] datevals=dateval.split("-");
				
				if (datevals[0].trim().equals("date"))
				{
					thiscal.add(Calendar.DAY_OF_MONTH,Integer.parseInt("-"+datevals[1].trim()));
					
					return sdf.format(thiscal.getTime());
				}
				else if (datevals[0].trim().equals("hour"))
				{
					
					thiscal.add(Calendar.HOUR_OF_DAY,Integer.parseInt("-"+datevals[1].trim()));
					return sdf.format(thiscal.getTime());
					
				}
			}
			else if (dateval.contains("+"))
			{
				String[] datevals=dateval.split("+");
				
				if (datevals[0].trim().equals("date"))
				{
					thiscal.add(Calendar.DAY_OF_MONTH,Integer.parseInt("+"+datevals[1].trim()));
					
					return sdf.format(thiscal.getTime());
				}
				else if (datevals[0].trim().equals("hour"))
				{
					thiscal.add(Calendar.HOUR_OF_DAY,Integer.parseInt("+"+datevals[1].trim()));
					return sdf.format(thiscal.getTime());
					
				}
			}
			else
			{
				
				
				if (dateval.trim().equals("date"))
				{
					thiscal.add(Calendar.DAY_OF_MONTH,Integer.parseInt("+"+dateval.trim()));
					
					return sdf.format(thiscal.getTime());
				}
				else if (dateval.trim().equals("hour"))
				{
					thiscal.add(Calendar.HOUR_OF_DAY,Integer.parseInt("+"+dateval.trim()));
					return sdf.format(thiscal.getTime());
					
				}
			}
			
		}
		return expr_date;
	}
	
	public static void main(String[] args)
	{
	  if (args.length==2)
	  {
		  System.out.println("args[0]:"+args[0]);
		  System.out.println("args[1]:"+args[1]);
		  sendmail(args[0],args[1]);
	  }
	}

}
