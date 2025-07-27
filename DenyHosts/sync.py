import logging
import os
import time
import sys
import socket
import requests

if sys.version_info < (3, 0): 
    from xmlrpclib import ServerProxy, Fault
else:
    from xmlrpc.client import ServerProxy, Transport, ProtocolError, Fault

from .constants import SYNC_TIMESTAMP, SYNC_HOSTS, SYNC_HOSTS_TMP, SYNC_RECEIVED_HOSTS, SOCKET_TIMEOUT

logging.basicConfig()
logger = logging.getLogger('sync')
debug, info, error, exception = logger.debug, logger.info, logger.error, logger.exception


def get_plural(items):
    if len(items) != 1:
        return "s"
    else:
        return ""


if sys.version_info >= (3, 0):
    class RequestsTransport(Transport):

        def request(self, host, handler, data, verbose=False):
            # set the headers, including the user-agent
            headers = {"User-Agent": "denyhosts-client",
                       "Content-Type": "text/xml",
                       "Accept-Encoding": "gzip"}
            url = "http://%s%s" % (host, handler)
            response = None
            
            # Log the request details for debugging
            debug("Making XMLRPC request to: %s", url)
            debug("Request headers: %s", headers)
            debug("Request timeout: %d seconds", SOCKET_TIMEOUT)
            
            # Don't log the full XML data as it can be verbose, but log its size
            if data:
                debug("Request data size: %d bytes", len(data))
            
            try:
                start_time = time.time()
                response = requests.post(url, data=data, headers=headers, timeout=SOCKET_TIMEOUT)
                request_duration = time.time() - start_time
                debug("Request completed in %.2f seconds", request_duration)
                
                debug("Response status code: %d", response.status_code)
                debug("Response headers: %s", dict(response.headers))
                
                response.raise_for_status()
                
            except requests.exceptions.ConnectTimeout as e:
                error("Connection timeout after %d seconds to %s: %s", SOCKET_TIMEOUT, url, str(e))
                error("This usually indicates network connectivity issues or server unavailability")
                raise ProtocolError(url, 408, "Connection timeout: " + str(e), {})
                
            except requests.exceptions.ReadTimeout as e:
                error("Read timeout after %d seconds from %s: %s", SOCKET_TIMEOUT, url, str(e))
                error("Server accepted connection but failed to respond in time")
                raise ProtocolError(url, 408, "Read timeout: " + str(e), {})
                
            except requests.exceptions.ConnectionError as e:
                error("Connection error to %s: %s", url, str(e))
                error("Check network connectivity and server availability")
                raise ProtocolError(url, 500, "Connection error: " + str(e), {})
                
            except requests.exceptions.HTTPError as e:
                error("HTTP error from %s: %s", url, str(e))
                if response is not None:
                    error("Response content: %s", response.text[:500])  # Log first 500 chars
                raise ProtocolError(
                    url,
                    response.status_code if response else 500,
                    "HTTP error: " + str(e),
                    response.headers if response else {}
                )
                
            except requests.RequestException as e:
                error("Request exception to %s: %s", url, str(e))
                if response is None:
                    raise ProtocolError(url, 500, "Request failed: " + str(e), {})
                else:
                    raise ProtocolError(
                        url,
                        response.status_code,
                        "Request failed: " + str(e),
                        response.headers
                    )
            
            if response is not None:
                debug("Response content size: %d bytes", len(response.text))
                return self.parse_response(response)
            else:
                error("Response is None - this should not happen")
                raise ProtocolError(url, 500, "Empty response received", {})

        def parse_response(self, resp):
            """
            Parse the xmlrpc response.
            """
            try:
                debug("Parsing XMLRPC response")
                p, u = self.getparser()
                p.feed(resp.text)
                p.close()
                result = u.close()
                debug("Successfully parsed XMLRPC response")
                return result
            except Exception as e:
                error("Failed to parse XMLRPC response: %s", str(e))
                error("Response content (first 500 chars): %s", resp.text[:500])
                raise


class Sync(object):
    def __init__(self, prefs):
        self.__prefs = prefs
        self.__work_dir = prefs.get('WORK_DIR')
        self.__connected = False
        self.__hosts_added = []
        self.__server = None
        self.__default_timeout = socket.getdefaulttimeout()
        self.__pymajor_version = sys.version_info[0]
        self.__sync_server = self.__prefs.get('SYNC_SERVER')

    def xmlrpc_connect(self):
        debug("xmlrpc_connect() - attempting to connect to: %s", self.__sync_server)
        
        if not self.__sync_server:
            error("SYNC_SERVER not configured")
            return False
            
        # python 2
        if self.__pymajor_version == 2:
            debug("Setting global socket timeout to %d seconds (Python 2)", SOCKET_TIMEOUT)
            socket.setdefaulttimeout(SOCKET_TIMEOUT)  # set global socket timeout
            
        for i in range(0, 3):
            debug("XMLRPC Connection attempt: %d/3", i + 1)
            try:
                start_time = time.time()
                
                # python 2
                if self.__pymajor_version == 2:
                    self.__server = ServerProxy(self.__sync_server)
                else:
                    self.__server = ServerProxy(self.__sync_server, transport=RequestsTransport())
                
                connection_time = time.time() - start_time
                debug("XMLRPC ServerProxy created in %.2f seconds", connection_time)
                info("Connected to SYNC Server: %s", self.__sync_server)
                self.__connected = True
                break
                
            except Exception as e:
                error("Connection attempt %d/3 failed: %s", i + 1, str(e))
                error("Exception type: %s", type(e).__name__)
                exception("Full exception details:")
                self.__connected = False
                
                if i < 2:  # Don't sleep on the last attempt
                    debug("Waiting 30 seconds before next attempt...")
                    time.sleep(30)
                    
        if not self.__connected:
            error('Failed to connect to %s after 3 attempts', self.__sync_server)
            error('Possible causes:')
            error('  1. Network connectivity issues')
            error('  2. Sync server is down or unreachable')
            error('  3. Firewall blocking connection')
            error('  4. DNS resolution failure')
            error('  5. Incorrect SYNC_SERVER configuration')
            
        # python 2
        if self.__pymajor_version == 2:
            debug("Restoring default socket timeout")
            socket.setdefaulttimeout(self.__default_timeout)  # set timeout back to the default
            
        return self.__connected

    def xmlrpc_disconnect(self):
        if self.__connected:
            try:
                debug("Disconnecting from XMLRPC server")
                # self.__server.close()
                self.__server = None
                debug("XMLRPC server disconnected")
            except Exception as e:
                debug("Exception during disconnect (usually harmless): %s", str(e))
            self.__connected = False

    def get_sync_timestamp(self):
        timestamp_file = os.path.join(self.__work_dir, SYNC_TIMESTAMP)
        debug("Reading sync timestamp from: %s", timestamp_file)
        
        timestamp = 0
        try:
            with open(timestamp_file) as fp:
                line = fp.readline().strip()
                if len(line) > 0:
                    timestamp = int(line)
                    debug("Found sync timestamp: %d (%s)", timestamp, time.ctime(timestamp))
                else:
                    debug("Sync timestamp file is empty, using 0")
                return timestamp
        except FileNotFoundError:
            debug("Sync timestamp file does not exist, using 0")
            return 0
        except ValueError as e:
            error("Invalid timestamp in file %s: %s", timestamp_file, str(e))
            return 0
        except Exception as e:
            error("Error reading sync timestamp: %s", str(e))
            return 0

    def set_sync_timestamp(self, timestamp):
        timestamp_file = os.path.join(self.__work_dir, SYNC_TIMESTAMP)
        debug("Writing sync timestamp %s to: %s", timestamp, timestamp_file)
        
        try:
            with open(timestamp_file, "w") as fp:
                fp.write(str(timestamp))
            debug("Sync timestamp written successfully")
        except Exception as e:
            error("Error writing sync timestamp to %s: %s", timestamp_file, str(e))

    def send_release_used(self, dh_version):
        debug('Attempting to send release version to sync server for tracking')
        debug('DenyHosts version: %s', dh_version)
        
        try:
            py_version = '.'.join([str(x) for x in sys.version_info[0:3]])
            version_info = [py_version, dh_version]
            debug('Version info to send: Python %s, DenyHosts %s', py_version, dh_version)
            
            if not self.__connected and not self.xmlrpc_connect():
                error("Could not initiate xmlrpc connection for version report")
                return

            for i in range(0, 3):
                debug("Version report attempt: %d/3", i + 1)
                try:
                    start_time = time.time()
                    result = self.__server.version_report(version_info)
                    call_duration = time.time() - start_time
                    debug("version_report call completed in %.2f seconds", call_duration)
                    
                    if result is not None:
                        debug("Version report successful, result: %s", str(result))
                    else:
                        debug("Version report successful (no return value)")
                    break
                    
                except Fault as f:
                    if 8001 == f.faultCode:
                        debug('version_report procedure doesn\'t exist on the sync server: %s', f.faultString)
                        debug('This is not an error - the server may not support version reporting')
                        break
                    else:
                        error('XMLRPC Fault during version report (attempt %d/3): Code %d, %s', 
                              i + 1, f.faultCode, f.faultString)
                        
                except ProtocolError as pe:
                    error('Protocol error during version report (attempt %d/3): %d %s', 
                          i + 1, pe.errcode, pe.errmsg)
                    error('URL: %s', pe.url)
                    if hasattr(pe, 'headers') and pe.headers:
                        error('Headers: %s', pe.headers)
                        
                except Exception as e:
                    error("Unexpected error during version report (attempt %d/3): %s", i + 1, str(e))
                    error("Exception type: %s", type(e).__name__)
                    exception("Full exception details:")
                    
                if i < 2:  # Don't sleep on the last attempt
                    debug("Waiting 30 seconds before next version report attempt...")
                    time.sleep(30)
                    
        except Exception as e:
            error('Failure reporting your setup: %s', str(e))
            error('Exception type: %s', type(e).__name__)
            exception('Full exception details:')
            
        finally:
            self.xmlrpc_disconnect()

    def send_new_hosts(self):
        debug("send_new_hosts() - preparing to send new hosts to sync server")
        self.__hosts_added = []
        
        src_file = os.path.join(self.__work_dir, SYNC_HOSTS)
        dest_file = os.path.join(self.__work_dir, SYNC_HOSTS_TMP)
        
        debug("Source file: %s", src_file)
        debug("Temp file: %s", dest_file)
        
        try:
            os.rename(src_file, dest_file)
            debug("Successfully renamed %s to %s", src_file, dest_file)
        except OSError as e:
            debug("No hosts file to rename (this is normal): %s", str(e))
            return False

        hosts = []
        try:
            with open(dest_file, 'r') as fp:
                # less memory usage than using readlines()
                for line_num, line in enumerate(fp, 1):
                    host = line.strip()
                    if host:  # Skip empty lines
                        hosts.append(host)
                    debug("Read host %d: %s", line_num, host)
                        
            debug("Read %d hosts from file", len(hosts))
        except Exception as e:
            error("Error reading hosts from %s: %s", dest_file, str(e))
            return False

        try:
            self.__send_new_hosts(hosts)
            info("sent %d new host%s", len(hosts), get_plural(hosts))
            self.__hosts_added = hosts
        except Exception as e:
            error("Failed to send hosts: %s", str(e))
            try:
                os.rename(dest_file, src_file)
                debug("Restored original file after send failure")
            except OSError as restore_error:
                error("Failed to restore original file: %s", str(restore_error))
            return False

        try:
            os.unlink(dest_file)
            debug("Deleted temporary file: %s", dest_file)
        except OSError as e:
            debug("Could not delete temp file (non-critical): %s", str(e))

        return True

    def __send_new_hosts(self, hosts):
        debug("__send_new_hosts() - sending %d hosts", len(hosts))
        
        if not hosts:
            debug("No hosts to send")
            return
            
        if not self.__connected and not self.xmlrpc_connect():
            error("Could not initiate xmlrpc connection")
            raise Exception("Failed to connect to sync server")

        for i in range(0, 3):
            debug("Send hosts attempt: %d/3", i + 1)
            try:
                start_time = time.time()
                result = self.__server.add_hosts(hosts)
                call_duration = time.time() - start_time
                debug("add_hosts call completed in %.2f seconds", call_duration)
                
                if result is not None:
                    debug("Add hosts successful, result: %s", str(result))
                else:
                    debug("Add hosts successful (no return value)")
                break
                
            except Exception as e:
                error("Send hosts attempt %d/3 failed: %s", i + 1, str(e))
                error("Exception type: %s", type(e).__name__)
                exception("Full exception details:")
                
                if i < 2:  # Don't sleep on the last attempt
                    debug("Waiting 30 seconds before next send attempt...")
                    time.sleep(30)
                else:
                    raise  # Re-raise the exception on the last attempt

    def receive_new_hosts(self):
        debug("receive_new_hosts() - attempting to receive new hosts from sync server")

        data = self.__receive_new_hosts()
        if data is None:
            debug("No data received from sync server")
            return None

        try:
            debug("Processing received data: %s", str(data)[:200])  # Log first 200 chars
            
            if not isinstance(data, dict):
                error("Received data is not a dictionary: %s", type(data).__name__)
                return None
                
            if 'timestamp' not in data:
                error("Received data missing 'timestamp' field")
                return None
                
            if 'hosts' not in data:
                error("Received data missing 'hosts' field")
                return None
                
            timestamp = data['timestamp']
            hosts = data['hosts']
            
            debug("Received timestamp: %s (%s)", timestamp, time.ctime(float(timestamp)))
            debug("Received %d hosts", len(hosts))
            
            self.set_sync_timestamp(str(timestamp))
            info("received %d new host%s", len(hosts), get_plural(hosts))
            debug("hosts received: %s", hosts[:10] if len(hosts) > 10 else hosts)  # Log first 10
            
            self.__save_received_hosts(hosts, timestamp)
            return hosts
            
        except Exception as e:
            error("Error processing received hosts data: %s", str(e))
            error("Exception type: %s", type(e).__name__)
            exception("Full exception details:")
            return None

    def __receive_new_hosts(self):
        debug("__receive_new_hosts() - requesting new hosts from sync server")

        if not self.__connected and not self.xmlrpc_connect():
            error("Could not initiate xmlrpc connection")
            return None
            
        timestamp = self.get_sync_timestamp()
        sync_dl_threshold = self.__prefs.get("SYNC_DOWNLOAD_THRESHOLD")
        sync_dl_resiliency = self.__prefs.get("SYNC_DOWNLOAD_RESILIENCY")
        
        debug("Request parameters:")
        debug("  timestamp: %d (%s)", timestamp, time.ctime(timestamp) if timestamp > 0 else "never")
        debug("  download_threshold: %s", sync_dl_threshold)
        debug("  hosts_added: %d", len(self.__hosts_added))
        debug("  download_resiliency: %s", sync_dl_resiliency)
        
        data = None
        for i in range(0, 3):
            debug("Receive hosts attempt: %d/3", i + 1)
            try:
                start_time = time.time()
                data = self.__server.get_new_hosts(
                    timestamp,
                    sync_dl_threshold,
                    self.__hosts_added,
                    sync_dl_resiliency
                )
                call_duration = time.time() - start_time
                debug("get_new_hosts call completed in %.2f seconds", call_duration)
                
                if data is not None:
                    debug("Received data successfully")
                else:
                    debug("Received None from server (this may be normal)")
                break
                
            except Exception as e:
                error("Receive hosts attempt %d/3 failed: %s", i + 1, str(e))
                error("Exception type: %s", type(e).__name__)
                exception("Full exception details:")
                
                if i < 2:  # Don't sleep on the last attempt
                    debug("Waiting 30 seconds before next receive attempt...")
                    time.sleep(30)

        if data is None:
            error('Unable to retrieve data from the sync server after 3 attempts')
            
        return data

    def __save_received_hosts(self, hosts, timestamp):
        debug('__save_received_hosts() - saving %d hosts', len(hosts))
        
        received_file = os.path.join(self.__work_dir, SYNC_RECEIVED_HOSTS)
        debug('Saving to file: %s', received_file)
        
        try:
            timestr = time.ctime(float(timestamp))
            debug('Timestamp string: %s', timestr)
            
            with open(received_file, "a") as fp:
                for host in hosts:
                    fp.write("%s:%s\n" % (host, timestr))
                    debug('Saved host: %s at %s', host, timestr)
                    
            debug('Successfully saved %d received hosts', len(hosts))
            
        except IOError as e:
            error("IO error saving received hosts: %s", str(e))
            return
        except Exception as e:
            error("Unexpected error saving received hosts: %s", str(e))
            exception("Full exception details:")
            return