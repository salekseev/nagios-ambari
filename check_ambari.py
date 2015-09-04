#!/usr/bin/python
from nagioscheck import NagiosCheck, UsageError
from nagioscheck import PerformanceMetric, Status
import optparse
import pprint
import requests

try:
    import json
except ImportError:
    import simplejson as json

class AmbariAlertsHealthCheck(NagiosCheck):

    def __init__(self):

        NagiosCheck.__init__(self)
	self.useragent=''

        self.add_option('U', 'url', 'url', 'URL to Ambari')
        self.add_option('t', 'timeout', 'timeout', 'Timeout in seconds (default 15s)')
        self.add_option('u', 'username', 'username', 'Username')
        self.add_option('p', 'password', 'password', 'Password')

    def check(self, opts, args):
	# Ignore SSL Cert warnings
	requests.packages.urllib3.disable_warnings()

        url = opts.url
        username = opts.username
        password = opts.password
	timeout = opts.timeout

		
	cluster_url = r'%s/api/v1/clusters' % (url)

	if username and password:	
	    try:
       	        cluster_response = requests.get(cluster_url, auth=(username, password), verify=False)
            except requests.exceptions.RequestException as e:
	        raise Status('Unknown', ('requests error: %s' % e,))


        try:
            clusters_data = json.loads(cluster_response.content)
        except ValueError:
            raise Status('Unknown', ("API returned nonsense",))

        clusters = clusters_data['items']

        for cluster in clusters:
            cluster_name = cluster['Clusters']['cluster_name']

	    try:

	        alerts_response = requests.get('%s/api/v1/clusters/%s/alerts?fields=*&Alert/state.in(WARNING,CRITICAL,UNKNOWN)&sortBy=Alert/state'
					       % (url, cluster_name), auth=(username, password), verify=False)
	    except requests.exceptions.RequestException as e:
	        raise Status('Unknown', ('requests error: %s' % e,))

	    try:
	        alerts_data = json.loads(alerts_response.content)
	    except ValueError, e:
		raise Status('Unknown', ("API returned nonsense",))

	    alerts = alerts_data['items']

	criticals = 0
        critical_details = []
        warnings = 0
        warning_details = []
	unknowns = 0
	unknown_details = []

        for alert_data in alerts:
	    alert = alert_data['Alert']
	    if alert['state'] == 'CRITICAL':
		criticals += 1
                critical_details.append("Cluster %s [%s]: %s"
                                        % (alert['cluster_name'], alert['component_name'], alert['text']))
            elif alert['state'] == 'WARNING':
                warnings += 1
                warning_details.append("Cluster %s [%s]: %s"
                                        % (alert['cluster_name'], alert['component_name'], alert['text']))
	    elif alert['state'] == 'UNKNOWN':
                unknowns += 1
                unknown_details.append("Cluster %s [%s]: %s"
                                        % (alert['cluster_name'], alert['component_name'], alert['text']))


        if criticals > 0:
            raise Status("Critical",
                         "There are critical errors: \r\n%s"
                         % (str("\r\n".join(critical_details)))
			)
        elif warnings > 0:
	    raise Status("Warning",
                         "There are warnings: \r\n%s"
                         % (str("\r\n".join(warning_details)))
			)
	elif unknowns > 0:
	    raise Status("Unknown",
                         "There are unknowns: \r\n%s"
                         % (str("\r\n".join(unknown_details)))
			)
        else:
            raise Status("OK", "No alerts.")

if __name__ == "__main__":
    AmbariAlertsHealthCheck().run()
