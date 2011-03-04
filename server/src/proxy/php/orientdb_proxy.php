<?php
/**
 *   Copyright (c) 2011 Dino Ciuffetti, NuvolaBase, TuxWeb S.r.l.
 *   
 *   ajax_proxy is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Lesser General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   ajax_proxy is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License
 *   along with ajax_proxy.  If not, see <http://www.gnu.org/licenses/>.
*/

/**
 *  Welcome to ajax_proxy, the simple HTTP Proxy for cross domain ajax requests.
 *  This is a simple and secure PHP script that aims to resolve the cross domain security
 *  enforcement imposed by web browsers. That is done by handling the cross domain javascript
 *  HTTP request locally. In other words, the browser will let your javascript code to GET/POST
 *  the given uri because it's your PHP driven server to accomplish with the request to the real server.
 *  
 *  For security reasons ajax_proxy do not let your javascript to choose the destination server
 *  at runtime, but that's imposed on a configuration variable.
 *  We must do this because otherwise your server would become an open proxy, and that is absolutely not desiderable!
 *
 *  The script is also good with HTTP GET/POST/PUT/DELETE requests, for example for REST.
 */

// You have to define here your real destination server without a trailing slash following this syntax:
// <http|https>://<user>:<password>@<host>
// Example: http://jack:bauer@dashboard.tuxweb.it
$real_destination_host = "https://user:password@db1.nuvolabase.com";

// We split the response into small chunks (in kbyte) so that the browser (javascript) can parse data while we are
// reading the response from the server. Do not set it too small, 32 is ok for most cases!
$chunk_size_kb = 32;

// User-Agent for the client side of the proxy. The real server will see this User-Agent instead of the client's one.
// If this is left empty, the real server will see the User-Agent of the client.
// $user_agent = "ajax_proxy HTTP agent";
$user_agent = 'ajax_proxy cross domain PHP script';

// Server side request timeout in ms. If the browser (javascript) does not complete the request into this timeout the
// connection with the client will be aborted. Please note that timeout cannot exceed PHP limits (php.ini).
// If the variable is setted to 0 the timeout will be disabled.
$request_client_timeout_ms = 6000;

// Client side response timeout in ms. If the destination server does not complete the response into this timeout the
// connection with the server will be aborted. Please note that timeout cannot exceed PHP limits (php.ini).
// If the variable is setted to 0 the timeout will be disabled.
// WARNING: you may send partial data to the browser (javascript) in case of timeout
// (the connection will be closed before finish to complete the read from the destination server).
$response_server_timeout_ms = 6000;


// CLIENT SIDE PROXY (open and handle the request to the backend real HTTP server and return back headers and body)
function send_datachunk_to_client($arg) { // this callback is assigned by ob_start and invoked on every ob_flush
	// if we like it we can modify the data before sending it to the browser... but now we don't.
	return false;
}
function check_timeout($handle) { // check if there was a timeout error with the backend server
	$info = @stream_get_meta_data($handle);
	if ($info['timed_out']) {
		return true; // there was a timeout
	} else {
		return false; // there was not a timeout
	}
}
function strip_header($h) { // strip out any HTTP backend server's response header we don't like
	$to_be_stripped = array("Content-Length", "Connection", "Accept-Ranges", "Date");
	$name = substr($h, 0, strpos($h, ":"));
	if ($name == "") { // this is a header without ":", return it full
		return $h;
	}
	if (in_array($name, $to_be_stripped)) { // this is a header that we don't like so we return false
		return false;
	}
	$value = substr($h, strpos($h, ":")+2);
	return "$name: $value"; // that's a header we like (may be...) so we are returning it
}
function handle_response_timeout($type) { // handle a timeout error. If type == 0 the problem is on header, else it's on body
	switch ($type) {
	case 0: // timeout connecting or reading headers
		generate_http_error("HTTP/1.1 500 Timeout waiting for destination server");
		break;
	default:
	case 1: // timeout on body read
		generate_http_error("HTTP/1.1 500 Timeout waiting for destination server");
		break;
	}
	return true;
}
function parse_headers($handle) { // here we parse HTTP backend server's response headers
	$info = @stream_get_meta_data($handle);
	if ($info['timed_out']) { // there was a connection/read timeout
		handle_response_timeout(0);
		return false;
	}
	for ($i=0; $i<count($info['wrapper_data']); $i++) { // for each response header, check if we like it or not
		$h = $info['wrapper_data'][$i];
		$hd = strip_header($h);
		if ($hd) { // we like it, and so we decide to pass it to the browser
			Header ($hd);
		}
	}
	// we add our custom headers here
	Header ("Connection: close");
	return $info;
}
function proxypass($url, $method, $parameters, $header_timeout_ms, $body_timeout_ms) {
	global $user_agent, $chunk_size_kb;
	
	$opts = array();
	$opts['http'] = array();
	$opts['http']['method'] = $method;
	if ($user_agent == "") { // setting the user agent according to the user choise
		$opts['http']['user_agent'] = $_SERVER['HTTP_USER_AGENT'];
	} else {
		$opts['http']['user_agent'] = $user_agent;
	}
	if (($method == "POST") || ($method == "PUT") || ($method == "DELETE")) {
		$opts['http']['content'] = http_build_query($parameters);
	}
	
	$context = stream_context_create($opts);
	
	$handle = @fopen($url, "rb", false, $context);
	if (!$handle) {
		@fclose($handle);
		generate_http_error("HTTP/1.1 500 Backend Server Error");
		echo ": caused by: ";
		echo $http_response_header[0];
		// echo " opening url: $url\n"; // security issue here because the password is printed in the output
		echo " opening url: " . parse_url($url, PHP_URL_SCHEME) . "://_hidden_server_" . parse_url($url, PHP_URL_PATH);
		ob_end_flush();
		flush();
		exit;
	}
	if ($header_timeout_ms > 0) {
		@stream_set_timeout($handle, 0, $header_timeout_ms * 1000);
	}
	$info = parse_headers($handle);
	if ($body_timeout_ms > 0) {
		@stream_set_timeout($handle, 0, $body_timeout_ms * 1000);
	}
	while (!feof($handle)) {
		$res = @fread($handle, $chunk_size_kb); // read a chunk of data from the backend server
		if ($res === FALSE) {
			//generate_http_error("HTTP/1.1 500 Backend Server Error");
			ob_end_flush();
			flush();
			exit;
		}
		$i = check_timeout($handle); // check if there was a timeout error
		if (!$i) { // good! The server's response chunk was in time with our need
			print $res; // send it to the client
			ob_flush(); // be sure to send it to the client
			flush(); // be extra sure to send it to the client :-)
		} else { // mhhh... :-( there was a timeout reading data
			handle_response_timeout(1);
			break;
		}
	}
	@fclose($handle);
	ob_end_flush();
}
// END CLIENT SIDE PROXY

// SERVER SIDE PROXY (accept and handle the client request (browser/javascript)
function generate_http_error($err) {
	Header ($err);
	echo "\n";
	echo $err;
}
function get_client_headers() { // here we get client (browser, javascript) headers
	$headers = array();
	foreach ($_SERVER as $k => $v) {
		if (substr($k, 0, 5) == "HTTP_") {
			$k = str_replace('_', ' ', substr($k, 5));
			$k = str_replace(' ', '-', ucwords(strtolower($k)));
			$headers[$k] = $v;
		}
	}
	return $headers;
}
function parse_client_parameters($method) { // parsing a client HTTP request
	switch ($method) {
	case "POST":
		return $_POST;
		break;
	case "PUT":
		return $_POST;
		break;
	case "GET":
		return $_GET;
		break;
	case "HEAD":
		return $_GET;
		break;
	case "DELETE":
		return $_GET;
		break;
	}
}
// END SERVER SIDE PROXY
	

// SCRIPT STARTS HERE
ob_start("send_datachunk_to_client", $chunk_size_kb);
ini_set('user_agent', "MyUserAgent\r\nX-Powered-By: dAm2K");
if ($chunk_size_kb < 1) $chunk_size_kb = 1;
$chunk_size_kb = $chunk_size_kb * 1024;
	
// SERVER SIDE PROXY
$client_headers = array();
$client_headers = get_client_headers();
$parameters = parse_client_parameters($_SERVER['REQUEST_METHOD']);
if ($response_server_timeout_ms > 0) {
	ini_set("max_execution_time", $response_server_timeout_ms / 1000);
	ini_set("max_input_time", $response_server_timeout_ms / 1000);
}

// CLIENT SIDE PROXY
$url = $real_destination_host . $_SERVER['REQUEST_URI'];
$url = str_replace($_SERVER["SCRIPT_NAME"], "", $url);
$header_timeout_ms = $response_server_timeout_ms;
$body_timeout_ms = $response_server_timeout_ms;

// SENDING DATA TO THE SERVER AND GETTING BACK DATA FOR THE CLIENT
proxypass($url, $_SERVER['REQUEST_METHOD'], $parameters, $header_timeout_ms, $body_timeout_ms);

?>
