use std::collections::HashMap;
use std::io::{Error, ErrorKind, pipe};
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use either::Either;
use smol::io::{BufReader, BufWriter, copy, split};
use smol::net::{TcpListener, TcpStream};
use tracing::{debug, info, trace, warn};

use smol::{Unblock, io, prelude::*};

use argh::FromArgs;

type UpdateBarrier = Arc<Mutex<HashMap<String, u64>>>;

static GIT_CACHE_DIR: OnceLock<Utf8PathBuf> = OnceLock::new();

#[derive(FromArgs)]
/// Command line arguments
struct Args {
    /// address to listen on, default: 127.0.0.1:9418
    #[argh(option, short = 'l', default = "String::from(\"127.0.0.1:9418\")")]
    listen_address: String,

    /// directory for git cache
    #[argh(option)]
    git_cache_dir: Option<String>,

    /// enable update barrier
    #[argh(switch, short = 'u')]
    update_barrier: bool,

    /// maximum amount of time in seconds to wait for a repository to be cloned, default: 10
    #[argh(option, default = "10")]
    clone_timeout: u32,
}

static UPDATE_BARRIER: AtomicU64 = AtomicU64::new(0);

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> io::Result<()> {
    let args: Args = argh::from_env();

    // initialize logging
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    smol::block_on(async_main(args))
}

async fn async_main(args: Args) -> io::Result<()> {
    // chose git cache directory
    git_cache_dir_set(args.git_cache_dir);
    info!("serving git cache from {}", GIT_CACHE_DIR.get().unwrap());

    // start listening
    let listener = TcpListener::bind(args.listen_address).await?;
    info!("listening on {}", listener.local_addr().unwrap());

    // set up update barrier
    if args.update_barrier {
        info!("update barrier enabled");
        UPDATE_BARRIER.store(now(), Release);
        update_barrier_handle_signal();
    }

    let update_barrier: UpdateBarrier = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                info!("accepted a connection from {addr}");
                let maybe_update_barrier = if args.update_barrier {
                    Some(update_barrier.clone())
                } else {
                    None
                };
                smol::spawn(handle_client(
                    stream,
                    maybe_update_barrier,
                    args.clone_timeout,
                ))
                .detach();
            }
            Err(e) => {
                warn!("accepting connection failed: {e}");
            }
        }
    }
}

fn update_barrier_handle_signal() {
    // SAFETY: within signal handler, only getting time and updating atomic
    unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGUSR1, || {
            let now = now();
            UPDATE_BARRIER.store(now, Release);
        })
        .unwrap();
    }
}

fn git_cache_dir_set(git_cache_dir_arg: Option<String>) {
    let git_cache_dir_str = if let Some(git_cache_dir_arg) = git_cache_dir_arg {
        git_cache_dir_arg
    } else if let Ok(git_cache_dir) = std::env::var("GIT_CACHE_DIR") {
        git_cache_dir
    } else {
        "~/.gitcache".into()
    };

    let git_cache_dir = Utf8PathBuf::from(&shellexpand::tilde(&git_cache_dir_str));

    GIT_CACHE_DIR.set(git_cache_dir).unwrap();
}

async fn handle_client(
    mut stream: TcpStream,
    mut update_barrier: Option<UpdateBarrier>,
    clone_timeout: u32,
) -> std::io::Result<()> {
    let client = stream.peer_addr().unwrap();

    let GitRequest {
        host,
        path,
        version_string,
    } = parse_request(&mut stream).await?;

    let url = format!("https://{host}{path}");

    info!("{client} requests {url}");

    if update_barrier_check(update_barrier.as_ref(), &url) {
        info!("updating {url}");
        prefetch(&url, clone_timeout).await?;
        update_barrier_update(update_barrier.as_mut().as_deref(), url);
    } else {
        info!("{url} was updated since update barrier, skipping update");
    }

    info!("{client} serving repo");
    upload_pack(stream, host, path, version_string).await?;

    Ok(())
}

fn update_barrier_check(update_barrier: Option<&UpdateBarrier>, url: &str) -> bool {
    if let Some(update_barrier) = update_barrier
        && let Some(instant) = update_barrier.lock().unwrap().get(url)
        && *instant >= UPDATE_BARRIER.load(Acquire)
    {
        false
    } else {
        true
    }
}

fn update_barrier_update(update_barrier: Option<&UpdateBarrier>, url: String) {
    if let Some(update_barrier) = update_barrier {
        update_barrier.lock().unwrap().insert(url, now());
    }
}

async fn prefetch(url: &str, clone_timeout: u32) -> Result<(), Error> {
    let mut command = Command::new("git")
        .env(
            "GIT_CONFIG_GLOBAL",
            format!("{}/config", GIT_CACHE_DIR.get().unwrap()),
        )
        .env("GIT_CONFIG_NOSYSTEM", "true")
        .args(["cache", "prefetch", "-U", url])
        .spawn()?;

    for _ in 0..clone_timeout * 10 {
        if command.try_wait()?.is_some() {
            trace!("child reaped");
            break;
        }
        smol::Timer::after(Duration::from_millis(100)).await;
    }

    Ok(())
}

async fn upload_pack(
    stream: TcpStream,
    host: String,
    path: Utf8PathBuf,
    version_string: Option<String>,
) -> Result<(), Error> {
    let mut path = Utf8PathBuf::from(&format!("{}/{host}{path}", GIT_CACHE_DIR.get().unwrap()));
    path.set_extension("git");

    info!("spawning git-upload-pack");

    let peer = stream.peer_addr().unwrap();

    let mut command = smol::process::Command::new("git-upload-pack")
        .env(
            "GIT_PROTOCOL",
            version_string.as_ref().map_or("version=0", |v| v),
        )
        .env("GIT_CONFIG_COUNT", "3")
        .env("GIT_CONFIG_KEY_0", "uploadpack.allowAnySHA1InWant")
        .env("GIT_CONFIG_VALUE_0", "true")
        .env("GIT_CONFIG_KEY_1", "uploadpack.allowFilter")
        .env("GIT_CONFIG_VALUE_1", "true")
        .env("GIT_CONFIG_KEY_2", "uploadpack.allowRefInWant")
        .env("GIT_CONFIG_VALUE_2", "true")
        .args(["--strict", path.as_str()])
        .stdin(smol::process::Stdio::piped())
        .stdout(smol::process::Stdio::piped())
        .spawn()?;

    let mut stdout_recv = command.stdout.take().unwrap();
    let mut stdin_send = command.stdin.take().unwrap();
    let (mut read, mut write) = split(stream);

    info!("starting git-upload-pack join");

    let combo = async { Either::Left(copy(&mut stdout_recv, &mut write).await) }
        .or(async { Either::Right(copy(&mut read, &mut stdin_send).await) });

    match combo.await {
        Either::Left(res) => {
            info!("git-upload-pack exited");
            let _ = write.close().await;
        }
        Either::Right(res) => {
            info!("peer closed connection");
            drop(stdin_send);
        }
    }

    info!("git-upload-pack join done");

    // if let Ok(in_bytes) = in_n
    //     && let Ok(out_bytes) = out_n
    // {
    //     info!("{peer}: in: {in_bytes}b, out: {out_bytes}b");
    // }

    let res = command.status().await?;
    if !res.success() {
        info!("git-upload-pack errored");
    }

    info!("git-upload-pack exited");
    Ok(())
}

struct GitRequest {
    host: String,
    path: Utf8PathBuf,
    version_string: Option<String>,
}

async fn parse_request(stream: &mut TcpStream) -> Result<GitRequest, Error> {
    fn bad_pkt() -> Error {
        Error::new(ErrorKind::InvalidData, "Malformed packet")
    }
    let mut buf: Vec<u8> = vec![0u8; 1024];
    let mut res;

    loop {
        info!("loop");
        // read
        res = stream.read(&mut buf).await?;
        info!("got {res} bytes");
        info!("buf: {}", buf[0..res].escape_ascii());
        let mut buf_pos = 0usize;
        buf_pos += res;
        if buf_pos == 0 {
            info!("connection with {} dropped", stream.peer_addr().unwrap());
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        trace!("got {buf_pos}b");
        if buf_pos < 4 {
            trace!("expected four bytes");
            return Err(Error::from(ErrorKind::InvalidData));
        }

        if &buf[0..4] == b"0000" {
            trace!("got flush-pkt");
            // clear
            buf.clear();
            continue;
        }

        let pkt_len_str = str::from_utf8(&buf[0..4]).map_err(|_| bad_pkt())?;
        let pkt_len = usize::from_str_radix(pkt_len_str, 16).map_err(|_| bad_pkt())?;

        while buf_pos < pkt_len {
            info!("read more");
            res = stream.read(&mut buf[buf_pos..]).await?;
            buf_pos += res;
        }

        if pkt_len == 4 {
            // clear
            buf.clear();
            continue;
        }

        let payload = &buf[4..];

        let parts: Result<Vec<&str>, _> = payload
            .split(|&item| item == 0)
            .filter(|part| !part.is_empty())
            .map(|part| str::from_utf8(part))
            .collect();

        let parts = parts.map_err(|_| bad_pkt())?;

        println!("parts: {parts:?}");
        let mut cmd_and_pathname = parts[0].split(' ');
        let request_command = cmd_and_pathname.next().ok_or_else(bad_pkt)?;
        let pathname = cmd_and_pathname.next().ok_or(bad_pkt())?;

        let mut version_string = None;

        for part in parts.iter().skip(1) {
            if part.starts_with("host=") {
            } else if part.starts_with("version=") {
                version_string = Some(part.to_string());
            }
        }

        if request_command != "git-upload-pack" {
            return Err(bad_pkt());
        }

        let (host, path) = split_hostname(Utf8Path::new(pathname));

        let request = GitRequest {
            host,
            path,
            version_string,
        };

        return Ok(request);
    }
}

fn split_hostname(path: &Utf8Path) -> (String, Utf8PathBuf) {
    let mut components: Vec<_> = path.components().collect();
    let host = if let Some(Utf8Component::RootDir) = components.first() {
        components.remove(1)
    } else {
        components.remove(0)
    };
    let path = components.iter().collect::<Utf8PathBuf>();

    (host.as_str().to_string(), path)
}
