use clap::{App, Arg};
use futures::future::join;
use futures::stream::StreamExt;
use log::{info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::io::Result as IoResult;
use std::net::Shutdown;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc;

const BUFFER_SIZE: usize = 4194304;

#[derive(Debug)]
enum ChannelMessage {
    // peer_id, seq, data, length
    DataToBuddy(u64, u64, Box<[u8]>, usize),
    // peer_id, seq
    EofToBuddy(u64, u64),
    // peer_id
    ResetToBuddy(u64),

    // peer_id, seq, data
    DataFromBuddy(u64, u64, Box<[u8]>),
    // peer_id, seq
    EofFromBuddy(u64, u64),
    // peer_id
    ResetFromBuddy(u64),

    // peer_id; ResetToBuddy implies InterfacerQuit
    InterfacerQuit(u64),
    // channel_id
    ChannelQuit(u64),

    NewPeer(TcpStream),
    NewBuddy(TcpStream),
}

impl ChannelMessage {
    async fn serialize_to<'a>(&self, w: &mut WriteHalf<'_>) -> IoResult<()> {
        match self {
            ChannelMessage::DataToBuddy(peer_id, seq, data, len) => {
                trace!(
                    "Writing packet, data, peer={}, seq={}, data_len={}",
                    peer_id,
                    seq,
                    len,
                );
                write_all(w, &[0u8]).await?;
                write_all(w, &peer_id.to_be_bytes()).await?;
                write_all(w, &seq.to_be_bytes()).await?;
                write_all(w, &len.to_be_bytes()).await?;
                write_all(w, &data.as_ref()[0usize..*len]).await?;
            }
            ChannelMessage::EofToBuddy(peer_id, seq) => {
                trace!("Writing packet, eof, peer={}, seq={}", peer_id, seq);
                write_all(w, &[1u8]).await?;
                write_all(w, &peer_id.to_be_bytes()).await?;
                write_all(w, &seq.to_be_bytes()).await?;
            }
            ChannelMessage::ResetToBuddy(peer_id) => {
                trace!("Writing packet, rst, peer={}", peer_id);
                write_all(w, &[2u8]).await?;
                write_all(w, &peer_id.to_be_bytes()).await?;
            }
            _ => panic!("Only Data/Eof/Reset can be serialized"),
        };
        Ok(())
    }

    async fn deserialize_from(r: &mut ReadHalf<'_>) -> IoResult<Self> {
        let ret = match read_exact(r, 1).await?[0] {
            0 => {
                let peer_id = read_u64(r).await?;
                let seq = read_u64(r).await?;
                let len = read_u64(r).await?;
                trace!(
                    "Reading packet, data, peer={}, seq={}, data_len={}",
                    peer_id,
                    seq,
                    len
                );
                ChannelMessage::DataFromBuddy(peer_id, seq, read_exact(r, len as usize).await?)
            }
            1 => {
                let peer_id = read_u64(r).await?;
                let seq = read_u64(r).await?;
                trace!("Reading packet, eof, peer={}, seq={}", peer_id, seq);
                ChannelMessage::EofFromBuddy(peer_id, seq)
            }
            2 => {
                let peer_id = read_u64(r).await?;
                trace!("Reading packet, rst, peer={}", peer_id);
                ChannelMessage::ResetFromBuddy(peer_id)
            }
            _ => unreachable!(),
        };
        Ok(ret)
    }
}

async fn write_all(w: &mut WriteHalf<'_>, data: &[u8]) -> IoResult<()> {
    let mut written_bytes = 0usize;
    let bytes = data.len();
    while written_bytes < bytes {
        let write_region = &data[written_bytes..bytes];
        let written_this_time = w.write(write_region).await?;
        written_bytes += written_this_time;
    }
    Ok(())
}

async fn read_exact(r: &mut ReadHalf<'_>, len: usize) -> IoResult<Box<[u8]>> {
    let mut data = vec![0u8; len].into_boxed_slice();
    let mut completed_bytes = 0usize;
    while completed_bytes < len {
        let region = &mut data[completed_bytes..len];
        let size = r.read(region).await?;
        completed_bytes += size;
    }
    Ok(data)
}

async fn read_u64(r: &mut ReadHalf<'_>) -> IoResult<u64> {
    let mut data = [0u8; 8];
    let mut completed_bytes = 0usize;
    while completed_bytes < 8 {
        let region = &mut data[completed_bytes..8];
        let size = r.read(region).await?;
        completed_bytes += size;
    }
    Ok(u64::from_be_bytes(data))
}

//                      CLIENT MODE
//        peer1 <-tcp-> [interfacer <-mpsc->                              ]
//        peer2 <-tcp-> [interfacer <-mpsc-> scheduler <-mpsc-> channel   ] <-tcp-> [channel/concierge
//     new peer <-tcp-> [ concierge <-mpsc->                              ]

//                      SERVER MODE
//     channel] <-tcp-> [   channel <-mpsc-> scheduler <-mpsc-> interfacer] <-tcp-> peer
// new channel] <-tcp-> [ concierge <-mpsc->                              ]

//      interfacer -------------ChannelMessage-------------> interfacer

// channel only forwards the ChannelMessage to the correct interfacer on the other side
// user and target also known as peer
// forwarders running in client/server pair refer each other as "buddy"

async fn interfacer(
    peer_id: u64,
    mut peer: TcpStream,
    mut to_schd: mpsc::Sender<ChannelMessage>,
    mut from_schd: mpsc::Receiver<ChannelMessage>,
) {
    let (mut from_peer, mut to_peer) = peer.split();
    let mut to_schd2 = to_schd.clone();
    let mut to_schd3 = to_schd.clone();

    let to_peer_worker = async move {
        loop {
            let msg = from_schd.recv().await.expect("mpsc channel error");
            // TODO reorder msg

            match msg {
                // write to peer
                ChannelMessage::DataFromBuddy(_, _, buffer) => {
                    trace!(
                        "Interfacer {} received data from buddy for peer, sending",
                        peer_id
                    );
                    // in case of write error, reset peer and inform buddy to reset
                    if let Err(e) = write_all(&mut to_peer, &buffer).await {
                        warn!("Write error for peer {}: {:?}", peer_id, e);
                        if let Err(e) = to_peer.as_ref().shutdown(Shutdown::Both) {
                            warn!("Failed to shutdown peer {}: {:?}", peer_id, e);
                        }
                        tokio::spawn(async move {
                            to_schd2
                                .send(ChannelMessage::ResetToBuddy(peer_id))
                                .await
                                .expect("failed to communicate with schd");
                        });
                        break;
                    };
                }
                // stop writing to peer and quit loop
                ChannelMessage::EofFromBuddy(_, _) => {
                    info!(
                        "Interfacer {} write to peer half close, eof send to peer",
                        peer_id
                    );
                    if let Err(e) = to_peer.as_ref().shutdown(Shutdown::Write) {
                        warn!("Failed to shutdown peer write {}: {:?}", peer_id, e);
                    }
                    break;
                }
                // shutdown peer and quit
                ChannelMessage::ResetFromBuddy(_) => {
                    info!(
                        "Interfacer {} write to peer half close, reset send to peer",
                        peer_id
                    );
                    if let Err(e) = to_peer.as_ref().shutdown(Shutdown::Both) {
                        warn!("Failed to shutdown peer write {}: {:?}", peer_id, e);
                    }
                    break;
                }
                _ => unreachable!(""),
            };
        }
    };

    let from_peer_worker = async move {
        loop {
            let mut buffer = vec![0u8; BUFFER_SIZE].into_boxed_slice();
            // read from peer
            let bytes = match from_peer.read(buffer.as_mut()).await {
                Ok(rsize) => rsize,
                // if reading fail, reset peer and tell buddy to reset, then stop reading
                Err(e) => {
                    trace!("Read error from peer at {}: {:?}", peer_id, e);
                    if let Err(e) = from_peer.as_ref().shutdown(Shutdown::Both) {
                        warn!("Failed to shutdown peer reader at {} :{:?}", peer_id, e);
                    }
                    if let Err(e) = to_schd.send(ChannelMessage::ResetToBuddy(peer_id)).await {
                        warn!("Failed to send message to muxer at {} :{:?}", peer_id, e);
                    }
                    break;
                }
            };

            // tell buddy to EOF if EOF received
            if bytes == 0 {
                info!("Interfacer to peer {} received EOF from peer", peer_id);
                if let Err(e) = from_peer.as_ref().shutdown(Shutdown::Read) {
                    warn!("Failed to shutdown reader(only) at {} :{:?}", peer_id, e);
                }
                if let Err(e) = to_schd.send(ChannelMessage::EofToBuddy(peer_id, 0)).await {
                    warn!("Failed to send message to muxer at {} :{:?}", peer_id, e);
                }
                break;
            }

            // send data to buddy via muxer
            if let Err(e) = to_schd
                .send(ChannelMessage::DataToBuddy(peer_id, 0, buffer, bytes))
                .await
            {
                warn!("Failed to send message to muxer at {} :{:?}", peer_id, e);
                break;
            };
        }
    };

    join(to_peer_worker, from_peer_worker).await;
    info!("Interfacer {} quiting voluntary", peer_id);
    to_schd3
        .send(ChannelMessage::InterfacerQuit(peer_id))
        .await
        .expect("failed to communicate with scheduler");
}

async fn channel(
    channel_id: u64,
    mut buddy: TcpStream,
    mut to_schd: mpsc::Sender<ChannelMessage>,
    mut from_schd: mpsc::Receiver<ChannelMessage>,
) {
    let (mut from_buddy, mut to_buddy) = buddy.split();
    let mut to_schd2 = to_schd.clone();

    let read_from_channel_worker = async move {
        loop {
            let msg = match ChannelMessage::deserialize_from(&mut from_buddy).await {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Channel {} failed to read: {:?}", channel_id, e);
                    return;
                }
            };
            to_schd
                .send(msg)
                .await
                .expect("Failed to communicate with scheduler");
        }
    };

    let write_to_channel_worker = async move {
        loop {
            let msg = from_schd
                .recv()
                .await
                .expect("Failed to communicate with scheduler");
            if let Err(e) = msg.serialize_to(&mut to_buddy).await {
                warn!("Channel {} failed to write: {:?}", channel_id, e);
                return;
            }
        }
    };
    join(read_from_channel_worker, write_to_channel_worker).await;
    info!("Channel {} quiting voluntary", channel_id);
    to_schd2
        .send(ChannelMessage::ChannelQuit(channel_id))
        .await
        .expect("failed to communicate with scheduler");
}

async fn client_mode_concierge(listen: &str, mut to_scheduler: mpsc::Sender<ChannelMessage>) {
    let mut listener = TcpListener::bind(listen)
        .await
        .expect("Failed to open client listen port");
    info!("Client concierge listening on {}", listen);
    let mut incoming = listener.incoming();
    while let Some(maybe_stream) = incoming.next().await {
        let stream = maybe_stream.expect("Failed to accept socket");
        if let Err(e) = to_scheduler.send(ChannelMessage::NewPeer(stream)).await {
            warn!("Failed to send message to scheduler: {:?}", e);
            break;
        }
    }
}

async fn server_mode_concierge(listen: &str, mut to_scheduler: mpsc::Sender<ChannelMessage>) {
    let mut listener = TcpListener::bind(listen)
        .await
        .expect("Failed to open server listen port");
    info!("Server concierge listening on {}", listen);
    let mut incoming = listener.incoming();
    while let Some(maybe_stream) = incoming.next().await {
        let stream = maybe_stream.expect("Failed to accept socket");
        if let Err(e) = to_scheduler.send(ChannelMessage::NewBuddy(stream)).await {
            warn!("Failed to send message to scheduler: {:?}", e);
            break;
        }
    }
}

fn select_channel(
    to_channels: &mut HashMap<u64, mpsc::Sender<ChannelMessage>>,
) -> &mut mpsc::Sender<ChannelMessage> {
    to_channels.get_mut(&0u64).unwrap()
}

async fn connect_and_select_channel<'a>(
    to_channels: &'a mut HashMap<u64, mpsc::Sender<ChannelMessage>>,
    channel_id: &mut u64,
    server: &str,
    to_schd_template: &mpsc::Sender<ChannelMessage>,
) -> &'a mut mpsc::Sender<ChannelMessage> {
    if to_channels.len() == 0 {
        info!("Connecting to buddy {}", server);
        let stream = TcpStream::connect(server)
            .await
            .expect("failed to connect to buddy");
        let (tx_to_channel, rx_to_channel) = mpsc::channel(1);
        let tx_to_schd = to_schd_template.clone();
        tokio::spawn(channel(*channel_id, stream, tx_to_schd, rx_to_channel));
        to_channels.insert(*channel_id, tx_to_channel);
        info!("New channel created, id={}", *channel_id);
        *channel_id += 1;
    }
    to_channels.get_mut(&0u64).unwrap()
}

fn select_interfacer(
    to_interfacers: &mut HashMap<u64, mpsc::Sender<ChannelMessage>>,
    peer_id: u64,
) -> &mut mpsc::Sender<ChannelMessage> {
    to_interfacers.get_mut(&peer_id).expect(&format!(
        "Message to interfacer {}, but interfacer not exists",
        peer_id
    ))
}

async fn connect_and_select_interfacer<'a>(
    to_interfacers: &'a mut HashMap<u64, mpsc::Sender<ChannelMessage>>,
    peer_id: u64,
    target: &str,
    to_schd_template: &mpsc::Sender<ChannelMessage>,
) -> IoResult<&'a mut mpsc::Sender<ChannelMessage>> {
    if !to_interfacers.contains_key(&peer_id) {
        info!(
            "Peer {} not found, opening new interfacer to {}",
            peer_id, target
        );
        let (tx_to_intf, rx_to_intf) = mpsc::channel(1);
        to_interfacers.insert(peer_id, tx_to_intf);
        let tcp = TcpStream::connect(target).await?;
        tokio::spawn(interfacer(
            peer_id,
            tcp,
            to_schd_template.clone(),
            rx_to_intf,
        ));
    }
    Ok(to_interfacers.get_mut(&peer_id).expect(&format!(
        "Message to interfacer {}, but interfacer not exists",
        peer_id
    )))
}

async fn scheduler_client(listen_on: &str, server: &str) {
    let (to_schd, mut reqs) = mpsc::channel(3);
    let concierge = client_mode_concierge(listen_on, to_schd.clone());
    info!("Forwarder client started, server addr: {}", server);
    let scheduler = async move {
        let mut to_channels = HashMap::new(); // a list of mpsc::Sender to channels
        let mut to_interfacers = HashMap::new(); // map peer_id to the mpsc::Sender of the corresponding interfacer
        let mut dead_peer_ids = HashSet::new();
        let mut peer_id = 5000u64;
        let mut channel_id = 0u64;
        loop {
            let msg = reqs.recv().await.expect("Scheduler failed to receive task");
            match msg {
                ChannelMessage::DataToBuddy(peer_id, _, _, _)
                | ChannelMessage::EofToBuddy(peer_id, _)
                | ChannelMessage::ResetToBuddy(peer_id) => {
                    trace!("Peer {} --> buddy", peer_id);
                    connect_and_select_channel(&mut to_channels, &mut channel_id, server, &to_schd)
                        .await
                        .send(msg)
                        .await
                        .unwrap_or_else(|_| {
                            warn!("schd cannot deliver msg to channel, maybe it's quit")
                        });
                }
                ChannelMessage::DataFromBuddy(peer_id, _, _)
                | ChannelMessage::EofFromBuddy(peer_id, _)
                | ChannelMessage::ResetFromBuddy(peer_id) => {
                    trace!("Buddy --> Peer {}", peer_id);
                    if dead_peer_ids.contains(&peer_id) {
                        warn!("Buddy had a message, but peer {} is already dead", peer_id);
                    } else {
                        select_interfacer(&mut to_interfacers, peer_id)
                            .send(msg)
                            .await
                            .unwrap_or_else(|_| {
                                warn!("schd cannot deliver msg to interfacer, maybe it's quit")
                            });
                    }
                }
                ChannelMessage::NewPeer(tcp) => {
                    // spawn new interfacer
                    let (tx_to_intf, rx_to_intf) = mpsc::channel(1);
                    tokio::spawn(interfacer(peer_id, tcp, to_schd.clone(), rx_to_intf));
                    to_interfacers.insert(peer_id, tx_to_intf);
                    info!("New peer {} connected", peer_id);
                    peer_id += 1;
                }
                ChannelMessage::NewBuddy(_) => {
                    unreachable!("NewBuddy should not be passed to client scheduler")
                }
                ChannelMessage::InterfacerQuit(dpeer_id) => {
                    info!("Peer {} quit", dpeer_id);
                    to_interfacers.remove(&dpeer_id);
                    dead_peer_ids.insert(dpeer_id);
                }
                ChannelMessage::ChannelQuit(dchannel_id) => {
                    info!("Channel {} quit", dchannel_id);
                    to_channels.remove(&dchannel_id);
                }
            }
        }
    };
    join(concierge, scheduler).await;
}

async fn scheduler_server(listen_on: &str, target: &str) {
    let (to_schd, mut reqs) = mpsc::channel(3);
    let concierge = server_mode_concierge(listen_on, to_schd.clone());
    info!("Forwarder server started, targeting {}", target);
    let scheduler = async move {
        let mut to_channels = HashMap::new(); // a list of mpsc::Sender to channels
        let mut to_interfacers = HashMap::new(); // map peer_id to the mpsc::Sender of the corresponding interfacer
        let mut dead_peer_ids = HashSet::new();
        let mut channel_id = 0u64;
        loop {
            let msg = reqs.recv().await.expect("Scheduler failed to receive task");
            match msg {
                ChannelMessage::DataToBuddy(peer_id, _, _, _)
                | ChannelMessage::EofToBuddy(peer_id, _)
                | ChannelMessage::ResetToBuddy(peer_id) => {
                    trace!("Peer {} --> buddy", peer_id);
                    select_channel(&mut to_channels)
                        .send(msg)
                        .await
                        .unwrap_or_else(|_| {
                            warn!("schd cannot deliver msg to channel, maybe it's quit")
                        });
                }
                ChannelMessage::DataFromBuddy(peer_id, _, _)
                | ChannelMessage::EofFromBuddy(peer_id, _)
                | ChannelMessage::ResetFromBuddy(peer_id) => {
                    trace!("Buddy --> Peer {}", peer_id);
                    if dead_peer_ids.contains(&peer_id) {
                        warn!("Buddy had a message, but peer {} is already dead", peer_id);
                    } else {
                        match connect_and_select_interfacer(
                            &mut to_interfacers,
                            peer_id,
                            target,
                            &to_schd,
                        )
                        .await
                        {
                            Ok(intf) => intf.send(msg).await.unwrap_or_else(|_| {
                                warn!("schd cannot deliver msg to interfacer, maybe it's quit")
                            }),
                            Err(e) => {
                                warn!("Failed to connect to target: {:?}", e);
                                select_channel(&mut to_channels)
                                    .send(ChannelMessage::ResetToBuddy(peer_id))
                                    .await
                                    .expect("failed to talk to channel");
                            }
                        };
                    }
                }
                ChannelMessage::NewBuddy(tcp) => {
                    assert_eq!(to_channels.len(), 0);
                    info!("New buddy connecting");
                    // spawn new channel
                    let (tx_to_chan, rx_to_chan) = mpsc::channel(1);
                    tokio::spawn(channel(channel_id, tcp, to_schd.clone(), rx_to_chan));
                    to_channels.insert(channel_id, tx_to_chan);
                    channel_id += 1;
                }
                ChannelMessage::NewPeer(_) => {
                    unreachable!("NewPeer should not be passed to server scheduler")
                }
                ChannelMessage::InterfacerQuit(dpeer_id) => {
                    info!("Peer {} quit", dpeer_id);
                    to_interfacers.remove(&dpeer_id);
                    dead_peer_ids.insert(dpeer_id);
                }
                ChannelMessage::ChannelQuit(dchannel_id) => {
                    info!("Channel {} quit", dchannel_id);
                    to_channels.remove(&dchannel_id);
                }
            }
        }
    };
    join(concierge, scheduler).await;
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let matches = App::new("r05-multi-path-tcp-forwarder")
        .version("0.1")
        .author("RecursiveG")
        .about("Forward a tcp stream")
        .arg(
            Arg::with_name("is-server")
                .short("s")
                .long("is-server")
                .help("Run in server mode")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("target")
                .short("t")
                .long("target")
                .value_name("IP:PORT")
                .help(
                    "The port of destination (server mode) or server listen port (non-server mode)",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("listen-on")
                .short("l")
                .long("listen-on")
                .value_name("IP:PORT")
                .takes_value(true),
        )
        .get_matches();
    info!("Parameters = {:?}", matches);
    if matches.is_present("is-server") {
        let target = matches
            .value_of("target")
            .expect("No target address specified in server mode");
        let listen = matches.value_of("listen-on").unwrap_or("[::]:9987");
        scheduler_server(listen, target).await;
    } else {
        let server_port = matches.value_of("target").unwrap_or("[::]:9987");
        let listen = matches.value_of("listen-on").unwrap_or("[::]:8888");
        scheduler_client(listen, server_port).await;
    }
}
