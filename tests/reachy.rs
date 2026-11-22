//! The bundled Reachy Mini channel adapter (resources/adapters/reachy.py)
//! driven on its own: the host's side over stdio, the robot's side over the
//! adapter's HTTP service on loopback, and `ctl` as a command. No robot, voice
//! provider or outside network is involved; the body app
//! (resources/devices/reachy) needs the robot and is checked on one.
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::{Duration, Instant},
};
use tempfile::TempDir;

/// The smallest thing the adapter takes for a camera frame: a JPEG's first bytes.
const JPEG: &str = "/9j/4AAQSkZJRg==";

/// A synthetic voice provider on loopback. It answers the request for a
/// session key and records what it was asked.
const VOICE_PROVIDER: &str = r#"
import http.server, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length') or 0)))
        (root / 'asked.json').write_text(json.dumps(
            {'path': self.path, 'authorization': self.headers.get('Authorization'), 'model': body['session']['model']}))
        data = json.dumps({'value': 'eph-synthetic-0001'}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
(root / 'port.tmp').write_text(str(server.server_address[1]))
(root / 'port.tmp').rename(root / 'port')
server.serve_forever()
"#;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/adapters/reachy.py")
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(30));
    }
}

fn port_in(folder: &Path) -> u16 {
    let mut port = String::new();
    until("a port file", || {
        port = fs::read_to_string(folder.join("port")).unwrap_or_default();
        port.ends_with('\n') || port.parse::<u16>().is_ok()
    });
    port.trim().parse().unwrap()
}

struct Stopped(Child);

impl Drop for Stopped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The adapter with the host's end of its stdio, as `oppa serve` holds it.
struct Adapter {
    _child: Stopped,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    state: TempDir,
    port: u16,
    token: String,
    next_id: u64,
}

impl Adapter {
    fn start(voice_url: &str) -> Self {
        let state = tempfile::tempdir().unwrap();
        let mut child = Command::new("python3")
            .arg(script())
            .args([
                "--port",
                "0",
                "--bind",
                "127.0.0.1",
                "--voice-url",
                voice_url,
            ])
            .args(["--voice-key", "/bin/echo synthetic-provider-key", "--state"])
            .arg(state.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let port = port_in(state.path());
        let token = fs::read_to_string(state.path().join("token"))
            .unwrap()
            .trim()
            .to_owned();
        let mut adapter = Self {
            _child: Stopped(child),
            stdin,
            stdout,
            state,
            port,
            token,
            next_id: 0,
        };
        let ready = adapter.call(
            "initialize",
            json!({"protocol_version": 1, "account": "home"}),
        );
        assert_eq!(
            ready["result"]["capabilities"]["attachments"], false,
            "{ready}"
        );
        adapter
    }

    fn frame(&mut self) -> Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap_or_else(|_| panic!("not a frame: {line}"))
    }

    /// One request from the host and its answer.
    fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let request =
            json!({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params});
        writeln!(self.stdin, "{request}").unwrap();
        let answer = self.frame();
        assert_eq!(answer["id"], self.next_id, "{answer}");
        answer
    }

    /// One HTTP exchange as the body app makes it; `token` None leaves the header out.
    fn http(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let body = body.map(|value| value.to_string()).unwrap_or_default();
        let authorization = token
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: robot\r\nConnection: close\r\n{authorization}Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut raw = String::new();
        stream.read_to_string(&mut raw).unwrap();
        let (head, text) = raw
            .split_once("\r\n\r\n")
            .unwrap_or_else(|| panic!("no body: {raw}"));
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (
            status,
            serde_json::from_str(text).unwrap_or_else(|_| panic!("not JSON: {text}")),
        )
    }

    fn robot(&self, method: &str, path: &str, body: Option<Value>) -> Value {
        let (status, answer) = self.http(method, path, Some(&self.token), body);
        assert_eq!(status, 200, "{method} {path}: {answer}");
        answer
    }

    /// What the robot is owed, as its poll returns it, without the long wait.
    fn poll(&self, after: u64, epoch: &str) -> Value {
        self.robot(
            "GET",
            &format!("/v1/next?after={after}&epoch={epoch}&state=asleep&wait=0"),
            None,
        )
    }

    fn ctl(&self, args: &[&str]) -> (bool, String) {
        let output = Command::new("python3")
            .arg(script())
            .args(["ctl", "--state"])
            .arg(self.state.path())
            .args(args)
            .output()
            .unwrap();
        let said = if output.status.success() {
            output.stdout
        } else {
            output.stderr
        };
        (
            output.status.success(),
            String::from_utf8_lossy(&said).trim().to_owned(),
        )
    }
}

/// V-1 and V-6: what the voice model hands over is one message from the
/// handle `room` in the conversation `home`, with the camera frame as a file
/// when one came along; a reply from the host is owed to the robot as words to
/// say. The token guards every route, and a frame must be a JPEG.
#[test]
fn the_room_speaks_as_one_handle_and_a_reply_is_words_for_the_robot() {
    let mut adapter = Adapter::start("http://127.0.0.1:9/v1");
    let (status, _) = adapter.http("GET", "/v1/next?wait=0", None, None);
    assert_eq!(status, 401);
    let (status, _) = adapter.http(
        "POST",
        "/v1/hand",
        Some("wrong-token"),
        Some(json!({"text": "hello"})),
    );
    assert_eq!(status, 401);

    let first = adapter.poll(0, "");
    assert_eq!(first["items"], json!([]));
    assert_eq!(
        adapter.call("health", json!({}))["result"],
        json!({"ok": true, "robot": "asleep"})
    );

    adapter.robot(
        "POST",
        "/v1/hand",
        Some(json!({"text": "Is the garage door closed?", "jpeg": JPEG})),
    );
    let event = adapter.frame();
    assert_eq!(event["method"], "event", "{event}");
    let params = &event["params"];
    assert_eq!(
        params["conversation"],
        json!({"native_id": "home", "kind": "dm", "title": "Home robot"})
    );
    assert_eq!(
        params["sender"],
        json!({"handle": "room", "display_name": null, "is_me": false})
    );
    assert_eq!(params["body"], "Is the garage door closed?");
    assert_eq!(params["attachments"][0]["mime_type"], "image/jpeg");
    let frame = PathBuf::from(params["attachments"][0]["path"].as_str().unwrap());
    assert!(
        frame.starts_with(adapter.state.path().join("frames")),
        "{frame:?}"
    );
    assert_eq!(fs::read(&frame).unwrap()[..3], [0xff, 0xd8, 0xff]);
    let (status, refused) = adapter.http(
        "POST",
        "/v1/hand",
        Some(&adapter.token),
        Some(json!({"text": "look", "jpeg": "bm90IGEganBlZw=="})),
    );
    assert_eq!(
        (status, refused["error"].as_str()),
        (400, Some("The frame is not a JPEG."))
    );

    let sent = adapter.call(
        "send",
        json!({"conversation": "home", "body": "Yes, it is closed."}),
    );
    assert!(
        sent["result"]["native_id"]
            .as_str()
            .unwrap()
            .starts_with("out-"),
        "{sent}"
    );
    let owed = adapter.poll(
        first["cursor"].as_u64().unwrap(),
        first["epoch"].as_str().unwrap(),
    );
    assert_eq!(
        owed["items"],
        json!([{"kind": "say", "text": "Yes, it is closed."}])
    );
    // Taken once: the next poll from that cursor has nothing.
    let after = adapter.poll(
        owed["cursor"].as_u64().unwrap(),
        owed["epoch"].as_str().unwrap(),
    );
    assert_eq!(after["items"], json!([]));

    let elsewhere = adapter.call("send", json!({"conversation": "kitchen", "body": "Hello."}));
    assert_eq!(elsewhere["error"]["code"], 1001, "{elsewhere}");
    let shown = adapter.call(
        "send",
        json!({"conversation": "home", "body": "See.", "attachments": [{"path": "/x"}]}),
    );
    assert_eq!(shown["error"]["code"], 1001, "{shown}");
}

/// A robot that is off or out of reach is told nothing later: a reply for it
/// is not sent, so the host knows, and `ctl` says why instead of queueing.
#[test]
fn a_robot_that_is_away_is_owed_nothing() {
    let mut adapter = Adapter::start("http://127.0.0.1:9/v1");
    assert_eq!(adapter.call("health", json!({}))["result"]["robot"], "away");
    let sent = adapter.call(
        "send",
        json!({"conversation": "home", "body": "Dinner is ready."}),
    );
    assert_eq!(
        sent["error"],
        json!({"code": 1001, "message": "the robot is not connected"})
    );
    assert_eq!(adapter.ctl(&["status"]), (true, "away".into()));
    let (ok, why) = adapter.ctl(&["wake", "Dinner", "is", "ready."]);
    assert!(
        !ok && why.starts_with("The robot is not connected"),
        "{why}"
    );
    assert_eq!(adapter.poll(0, "")["items"], json!([]));
}

/// The assistant steers the robot with `ctl`: wake with words to say, sleep,
/// and a look that waits for the frame the robot sends back and prints its path.
#[test]
fn ctl_wakes_the_robot_puts_it_to_sleep_and_brings_back_a_look() {
    let adapter = Adapter::start("http://127.0.0.1:9/v1");
    let first = adapter.poll(0, "");
    let epoch = first["epoch"].as_str().unwrap().to_owned();
    assert_eq!(
        adapter.ctl(&["wake", "Dinner", "is", "ready."]),
        (true, "asleep".into())
    );
    assert_eq!(adapter.ctl(&["wake"]), (true, "asleep".into()));
    adapter.robot("POST", "/v1/state", Some(json!({"state": "awake"})));
    assert_eq!(adapter.ctl(&["sleep"]), (true, "awake".into()));
    let orders = adapter.poll(0, &epoch);
    assert_eq!(
        orders["items"],
        json!([{"kind": "say", "text": "Dinner is ready."}, {"kind": "wake"}, {"kind": "sleep"}])
    );

    let cursor = orders["cursor"].as_u64().unwrap();
    let (ok, path) = std::thread::scope(|scope| {
        let looking = scope.spawn(|| adapter.ctl(&["look"]));
        let mut asked = Value::Null;
        until("the look to reach the robot", || {
            asked = adapter.poll(cursor, &epoch)["items"][0].clone();
            asked["kind"] == "look"
        });
        adapter.robot(
            "POST",
            "/v1/frame",
            Some(json!({"id": asked["id"], "jpeg": JPEG})),
        );
        looking.join().unwrap()
    });
    assert!(ok, "{path}");
    assert_eq!(fs::read(&path).unwrap()[..3], [0xff, 0xd8, 0xff]);
}

/// V-2: the body app gets a key that opens one voice session and the
/// session's whole set-up, never the provider's own key, which goes only to
/// the provider's address.
#[test]
fn a_voice_session_gets_a_short_lived_key_and_its_set_up_from_the_adapter() {
    let provider = tempfile::tempdir().unwrap();
    let _server = Stopped(
        Command::new("python3")
            .args(["-c", VOICE_PROVIDER])
            .arg(provider.path())
            .spawn()
            .unwrap(),
    );
    let port = port_in(provider.path());
    let adapter = Adapter::start(&format!("http://127.0.0.1:{port}/v1"));

    let (status, grant) =
        adapter.http("POST", "/v1/session", Some(&adapter.token), Some(json!({})));
    assert_eq!(status, 200, "{grant}");
    assert_eq!(grant["secret"], "eph-synthetic-0001");
    assert_eq!(
        grant["url"],
        format!("ws://127.0.0.1:{port}/v1/realtime?model=gpt-realtime-2.1")
    );
    assert_eq!(grant["idle_seconds"], 60);
    let tools: Vec<&str> = grant["session"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(tools, ["delegate", "look", "sleep"]);
    assert!(!grant.to_string().contains("synthetic-provider-key"));
    let asked: Value =
        serde_json::from_slice(&fs::read(provider.path().join("asked.json")).unwrap()).unwrap();
    assert_eq!(
        asked,
        json!({"path": "/v1/realtime/client_secrets", "authorization": "Bearer synthetic-provider-key",
            "model": "gpt-realtime-2.1"})
    );
}
