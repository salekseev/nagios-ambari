#!/usr/bin/python
from nagioscheck import NagiosCheck, UsageError
from nagioscheck import PerformanceMetric, Status
import optparse
import urllib.request, urllib.parse, urllib.error
import urllib.parse

try:
    import json
except ImportError:
    import simplejson as json


class AmbariAlertsHealthCheck(NagiosCheck):

    def __init__(self):

        NagiosCheck.__init__(self)

        self.add_option('U', 'url', 'url', 'URL to Ambari')
        self.add_option('u', 'username', 'username', 'Username')
        self.add_option('p', 'password', 'password', 'Password')

    def fetch(self, url, post=None, username=None, password=None, compress=True):

        parts = urllib.parse.urlparse(url)

        if username and password:
            password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            password_manager.add_password(None, url, username, password)
            password_auth = urllib.request.HTTPBasicAuthHandler(password_manager)
            self.build_opener(password_auth)
        urllib.request.install_opener(self.opener)

        if compress:
            headers = {'User-Agent' : self.useragent, 'Accept-encoding' : 'gzip'}
        else:
            headers = {'User-Agent' : self.useragent}

        req = urllib2.Request(url, post, headers)
        with gevent.Timeout(timeout):
            response = urllib2.urlopen(req)
            return HTTPResponse(response, url, http=self)

        urllib.request.urlopen(parts.geturl())

    def check(self, opts, args):
        url = opts.url
        username = opts.username
        password = opts.password

        response_body = fetch(r'%s/api/v1/clusters' % (url), username, password)

        try:
            clusters_data = json.loads(response_body)
        except ValueError:
            raise Status('unknown', ("API returned nonsense",))

        clusters = clusters_data['items']
        for cluster in clusters:
            cluster_name = cluster['Clusters']['cluster_name']
            pprint.pprint(cluster_name)

        try:
            response = urllib2.urlopen(r'%s//api/v1/clusters/hdphio/alerts?fields=*&Alert/state.in(WARNING,CRITICAL,UNKNOWN)&sortBy=Alert/state'
                                       % (url))
        except urllib2.HTTPError, e:
            raise Status('unknown', ("API failure", None,
                                     "API failure:\n\n%s" % str(e)))
        except urllib2.URLError, e:
            raise Status('critical', (e.reason))

        response_body = response.read()

        try:
            alerts_data = json.loads(response_body)
        except ValueError:
            raise Status('unknown', ("API returned nonsense",))

        criticals = 0
        critical_details = []
        warnings = 0
        warning_details = []

        nodes = nodes_jvm_data['nodes']
        for node in nodes:
            jvm_percentage = nodes[node]['jvm']['mem']['heap_used_percent']
            node_name = nodes[node]['host']
            if int(jvm_percentage) >= critical:
                criticals = criticals + 1
                critical_details.append("%s currently running at %s%% JVM mem "
                                        % (node_name, jvm_percentage))
            elif (int(jvm_percentage) >= warning and
                  int(jvm_percentage) < critical):
                warnings = warnings + 1
                warning_details.append("%s currently running at %s%% JVM mem "
                                       % (node_name, jvm_percentage))

        if criticals > 0:
            raise Status("Critical",
                         "There are '%s' node(s) in the cluster that have "
                         "breached the %% JVM heap usage critical threshold "
                         "of %s%%. They are:\r\n%s"
                         % (
                             criticals,
                             critical,
                             str("\r\n".join(critical_details))
                             ))
        elif warnings > 0:
            raise Status("Warning",
                         "There are '%s' node(s) in the cluster that have "
                         "breached the %% JVM mem usage warning threshold of "
                         "%s%%. They are:\r\n%s"
                         % (warnings, warning,
                            str("\r\n".join(warning_details))))
        else:
            raise Status("OK", "All nodes in the cluster are currently below "
                         "the % JVM mem warning threshold")

if __name__ == "__main__":
    AmbariAlertsHealthCheck().run()
