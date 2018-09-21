package main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.ASao;
import util.RDao;
import util.Sock;

public class Worker implements Callable<Boolean> {
	private static final Logger LOG = LogManager.getLogger(Worker.class);
	int thNo;
	int thAll;
	String rdbUrl;
	String rdbUser;
	String rdbPassword;
	int agentPort;
	int customPort;
	String customServiceName;
	String sql;
	int agentTimeout;

	public Worker(int thNo, int thAll, String rdbUrl, String rdbUser,
			String rdbPasswd, int agentPort, int customPort,
			String customServiceName, String sql, int agentTimeout) {
		this.thNo = thNo;
		this.thAll = thAll;
		this.rdbUrl = rdbUrl;
		this.rdbUser = rdbUser;
		this.rdbPassword = rdbPasswd;
		this.agentPort = agentPort;
		this.customPort = customPort;
		this.customServiceName = customServiceName;
		this.sql = sql;
		this.agentTimeout = agentTimeout;
	}

	@Override
	public Boolean call() throws Exception {
		RDao rDao = new RDao();
		Connection conn = rDao.getConnection(rdbUrl, rdbUser, rdbPassword);
		HashMap<String, String> hostKVstatus = new HashMap<String, String>();
		ArrayList<String> hosts = rDao.getHostsMT(conn, thNo - 1, thAll,
				hostKVstatus, customServiceName, sql);
		// ArrayList<String> hosts = rDao.getHostsTest();
		HashMap<String, Boolean> isV3 = rDao.getV3Info(conn);

		int i = 0;
		Sock sock = new Sock();
		DateTime start = new DateTime();

		ASao asao = new ASao();

		for (String host : hosts) {
			LOG.trace(thNo + "-" + i + ":Checking:" + host);
			boolean bPing = sock.isPingWorking(host);
			LOG.info("ping_try1:" + bPing);
			if (!bPing) {
				Thread.sleep(1000);
				bPing = sock.isPingWorking(host);
				LOG.info("ping_try2:" + bPing);
				if (!bPing) {
					Thread.sleep(1000);
					bPing = sock.isPingWorking(host);
					LOG.info("ping_try3:" + bPing);
					if (!bPing) {
						Thread.sleep(1000);
						bPing = sock.isPingWorking(host);
						LOG.info("ping_try4:" + bPing);
						if (!bPing) {
							Thread.sleep(1000);
							bPing = sock.isPingWorking(host);
							LOG.info("ping_try5:" + bPing);
						}
					}
				}
			}

			boolean bAgent = false;
			if (isV3.containsKey(host) && isV3.get(host)) {
				bAgent = asao.isWorking(host, agentPort, agentTimeout);
				LOG.info("agent_try1:" + bAgent);
				if (!bAgent) {
					Thread.sleep(1000);
					bAgent = asao.isWorking(host, agentPort, agentTimeout);
					LOG.info("agent_try2:" + bAgent);
					if (!bAgent) {
						Thread.sleep(1000);
						bAgent = asao.isWorking(host, agentPort, agentTimeout);
						LOG.info("agent_try3:" + bAgent);
						if (!bAgent) {
							Thread.sleep(1000);
							bAgent = asao.isWorking(host, agentPort,
									agentTimeout);
							LOG.info("agent_try4:" + bAgent);
							if (!bAgent) {
								Thread.sleep(1000);
								bAgent = asao.isWorking(host, agentPort,
										agentTimeout);
								LOG.info("agent_try5:" + bAgent);
							}
						}
					}
				}
			} else {
				bAgent = sock.isPortWorking(host, agentPort);
				LOG.info("agent_try1:" + bAgent);
				if (!bAgent) {
					Thread.sleep(1000);
					bAgent = sock.isPortWorking(host, agentPort);
					LOG.info("agent_try2:" + bAgent);
					if (!bAgent) {
						Thread.sleep(1000);
						bAgent = sock.isPortWorking(host, agentPort);
						LOG.info("agent_try3:" + bAgent);
						if (!bAgent) {
							Thread.sleep(1000);
							bAgent = sock.isPortWorking(host, agentPort);
							LOG.info("agent_try4:" + bAgent);
							if (!bAgent) {
								Thread.sleep(1000);
								bAgent = sock.isPortWorking(host, agentPort);
								LOG.info("agent_try5:" + bAgent);
							}
						}
					}
				}
			}
			boolean bCustom = sock.isPortWorking(host, customPort);
			LOG.info(customServiceName + "_try1:" + bCustom);
			if (!bCustom) {
				Thread.sleep(1000);
				bCustom = sock.isPortWorking(host, customPort);
				LOG.info(customServiceName + "_try2:" + bCustom);
				if (!bCustom) {
					Thread.sleep(1000);
					bCustom = sock.isPortWorking(host, customPort);
					LOG.info(customServiceName + "_try3:" + bCustom);
					if (!bCustom) {
						Thread.sleep(1000);
						bCustom = sock.isPortWorking(host, customPort);
						LOG.info(customServiceName + "_try4:" + bCustom);
						if (!bCustom) {
							Thread.sleep(1000);
							bCustom = sock.isPortWorking(host, customPort);
							LOG.info(customServiceName + "_try5:" + bCustom);
						}
					}
				}
			}

			boolean flgSysdate = true;
			String status = "unknown";
			if (!bAgent && !bCustom && !bPing) {
				status = "abnormal";
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null && !oldStatus.matches(status)
						&& oldStatus.matches("abnormalT")) {
					rDao.insertAlert(conn, host, oldStatus, status, "DOWN002");
				}
			}
			if (!bAgent && !bCustom && bPing) {
				status = "abnormal_agent_" + customServiceName;
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null
						&& !oldStatus.matches(status)
						&& (oldStatus.matches("abnormal_" + customServiceName
								+ "T") || oldStatus.matches("abnormal_agent_"
								+ customServiceName + "T"))) {
					rDao.insertAlert(conn, host, oldStatus, status, "HANGUP000");
				}
			}
			if (!bAgent && bCustom && !bPing)
				status = "abnormal_agent_ping";
			if (!bAgent && bCustom && bPing)
				status = "abnormal_agent";
			if (bAgent && !bCustom && !bPing)
				status = "abnormal_" + customServiceName + "_ping";
			if (bAgent && !bCustom && bPing) {
				status = "abnormal_" + customServiceName;
				String oldStatus = hostKVstatus.get(host);
				if (oldStatus != null
						&& !oldStatus.matches(status)
						&& (oldStatus.matches("abnormal_" + customServiceName
								+ "T") || oldStatus.matches("abnormal_agent_"
								+ customServiceName + "T"))) {
					rDao.insertAlert(conn, host, oldStatus, status, "HANGUP001");
				}
			}
			if (bAgent && bCustom && !bPing)
				status = "abnormal_ping";
			LOG.info(host + ":bAgent=" + bAgent + ",:bCustom=" + bCustom
					+ ",:bPing=" + bPing);
			if (!status.matches("unknown"))
				rDao.updateStatus(conn, host, status, flgSysdate);
			i++;
		}
		rDao.setWorkingTimestamp(conn, rdbUrl, thNo);
		DateTime end = new DateTime();
		Duration elapsedTime = new Duration(start, end);
		LOG.info(elapsedTime);
		rDao.disconnect(conn);
		return true;
	}
}
