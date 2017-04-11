package com.yiguan.kafka.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class DistinctAST {

	@SuppressWarnings({ "rawtypes", "unused", "unchecked" })
	public static String distinctAll(String jsonStrDouble) throws IOException {
		JSONObject jsonObject = null;
		JSONObject jsonObjectNew = null;
		List listNew = new ArrayList();
		String timeStr = null;
		String AET = "";
		JSONObject retunJson = new JSONObject();
		// 处理重复数据
		String[] ss = jsonStrDouble.split("-----");
		jsonObject = JSONObject.fromObject(ss[0]);
		String asdString = jsonObject.getString("AInfo");
		JSONArray jsonarray = JSONArray.fromObject(asdString);
		List list = (List) JSONArray.toCollection(jsonarray, Map.class);
		if (list.size() > 0) {
			timeStr = JSONObject.fromObject(list.get(0)).getString("AST");
		}
		for (int i = 1, j = 1, count = list.size(); i < count; i++) {
			long time = Long.parseLong(timeStr);
			long timeNew = Long.parseLong(JSONObject.fromObject(list.get(i)).getString("AST"));
			jsonObjectNew = JSONObject.fromObject(list.get(i - j));
			if (time + 60000 > timeNew) {
				AET = JSONObject.fromObject(list.get(i)).getString("AST");
				j++;
				jsonObjectNew.put("AET", AET);
			} else {
				jsonObjectNew.put("AET", AET);
				timeStr = JSONObject.fromObject(list.get(i)).getString("AST");
				AET = "";
				j = 1;
				listNew.add(jsonObjectNew);
			}
			if (i + j == count + 1) {
				jsonObjectNew.put("AET", AET);
				listNew.add(jsonObjectNew);
			}
		}
		// 拼接字符串
		jsonObject.put("AInfo", listNew);
		String tmpNew = jsonObject + "-----" + ss[1];
		return tmpNew;
	}

	public void writeToTxt(String data) throws IOException {
		FileOutputStream fos = new FileOutputStream(new File("E:\\1111\\new15"), true);
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
		BufferedWriter bw = new BufferedWriter(osw);
		bw.write(data + "\t\n");
		bw.close();
		osw.close();
		fos.close();
	}
}
