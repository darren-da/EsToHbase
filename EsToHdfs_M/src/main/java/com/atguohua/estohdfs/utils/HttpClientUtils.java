package com.atguohua.estohdfs.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpClientUtils {

	public static Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);

	private static PoolingHttpClientConnectionManager cm = null;

	/**
	 * 默认content 类型
	 */
	private static final String DEFAULT_CONTENT_TYPE = "application/x-www-form-urlencoded";

	/**
	 * 默认请求超时时间30s
	 */
	private static final int DEFAUL_TTIME_OUT = 15000;

	private static final int count = 32;

	private static final int totalCount = 1000;

	private static final int Http_Default_Keep_Time = 15000;

	private static CloseableHttpClient httpClient = null;
	/**
	 * 初始化连接池
	 */
	public static synchronized void initPools(){
		if(httpClient == null){
			cm = new PoolingHttpClientConnectionManager();
			// 将最大连接数增加到  
			cm.setMaxTotal(count); 
			// 将每个路由基础的连接增加到 
			cm.setDefaultMaxPerRoute(totalCount); 
			httpClient = HttpClients.custom().setKeepAliveStrategy(defaultStrategy).setConnectionManager(cm).build();
			logger.info("初始化HttpClients");
		}
	} 

	public static CloseableHttpClient getHttpClient() {
		return httpClient;
	}

	public static PoolingHttpClientConnectionManager getHttpConnectionManager() {
		return cm;
	}

	/**
	 * Http connection keepAlive 设置
	 */

	public static ConnectionKeepAliveStrategy defaultStrategy = new ConnectionKeepAliveStrategy() {

		public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
			HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
			int keepTime = Http_Default_Keep_Time;
			while (it.hasNext()) {
				HeaderElement he = it.nextElement();
				String param = he.getName();
				String value = he.getValue();
				if (value != null && param.equalsIgnoreCase("timeout")) {
					try {
						return Long.parseLong(value) * 1000;
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("format KeepAlive timeout exception, exception:" + e.toString());
					}
				}
			}
			return keepTime * 1000;
		}



	};


	/**
	 * 执行http post请求 默认采用Content-Type：application/json，Accept：application/json
	 *
	 * @param url 请求地址
	 * @param params  请求数据
	 * @return
	 */
	public static String execute(String url, Map<String, String> params) {
		long startTime = System.currentTimeMillis();
		HttpEntity httpEntity = null;
		HttpEntityEnclosingRequestBase method = null;
		String responseBody = "";
		try {
			if (httpClient == null) {
				initPools();
			}
			method = (HttpEntityEnclosingRequestBase) getRequest(url,  DEFAULT_CONTENT_TYPE, 0);
			//设置post请求参数
			List<NameValuePair> pairs = new ArrayList<NameValuePair>();
			for (String key: params.keySet()){
				pairs.add(new BasicNameValuePair(key, params.get(key)));
			}
			method.setEntity(new UrlEncodedFormEntity(pairs, "utf-8"));
			HttpClientContext context = HttpClientContext.create();
			CloseableHttpResponse httpResponse = httpClient.execute(method, context);
			httpEntity = httpResponse.getEntity();
			if (httpEntity != null) {
				responseBody = EntityUtils.toString(httpEntity, "UTF-8");
			}

		} catch (Exception e) {
			if(method != null){
				method.abort();
			}
			e.printStackTrace();
			logger.error(
					"execute post request exception, url:" + url + ", exception:" + e.toString() + ", cost time(ms):"
							+ (System.currentTimeMillis() - startTime));
		} finally {
			if (httpEntity != null) {
				try {
					EntityUtils.consumeQuietly(httpEntity);
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(
							"close response exception, url:" + url + ", exception:" + e.toString() + ", cost time(ms):"
									+ (System.currentTimeMillis() - startTime));
				}
			}
		}
		return responseBody;
	}

	/**
	 * 创建请求
	 *
	 * @param url 请求url
	 * @param contentType contentType类型
	 * @param timeout 超时时间
	 * @return
	 */
	public static HttpRequestBase getRequest(String url, String contentType, int timeout) {
		if (httpClient == null) {
			initPools();
		}

		if (timeout <= 0) {
			timeout = DEFAUL_TTIME_OUT;
		}
		RequestConfig requestConfig = RequestConfig.custom()
				.setSocketTimeout(timeout * 1000)
				.setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000)
				.setExpectContinueEnabled(false)
				.build();
		HttpRequestBase method = new HttpPost(url);
		if (StringUtils.isBlank(contentType)) {
			contentType = DEFAULT_CONTENT_TYPE;
		}
		method.addHeader("Content-Type", contentType);
		method.addHeader("Accept", "application/json");
		method.addHeader("Authorization", "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiIsImF1ZGllbmNlIjoiQVVESUVOQ0VfV0VCIiwiY3JlYXRlZCI6MTU0MjI4MzUyNTQ0NCwidXNlcm5hbWVDbiI6ImFkbWluIiwiZXhwIjo0NTQyMjgzNTI1LCJ1c2VyaWQiOiIxIn0.KoJsMRudL9WschK_ervYhP91r2LF54ujRSX-Z7KrxKshncPcxAuMn0YJGvq1bEReWn-FP2zIoZNyWdFiPk7O7A");
		method.setConfig(requestConfig);
		return method;
	}
}
