use anyhow::{Context, Result};
use camino::Utf8Path;
use image::io::Reader as ImageReader;
use memmap::Mmap;
use openh264::encoder::{Encoder, EncoderConfig};
use openh264::formats::YUVBuffer;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::reflect::FileDescriptor;
use protobuf::Message;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Cursor;
use std::sync::Arc;
use std::{env, fs};

mod foxglove {
    include!(concat!(env!("OUT_DIR"), "/generated_protos/mod.rs"));
}

fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

fn get_help_msg() -> String {
    let options = vec![
        ("-i, --input <FILE>", "Input MCAP file path (required)"),
        ("-o, --output <FILE>", "Output MCAP file path (default: compressed_video.mcap)"),
        ("--silent", "Disable verbose output. Errors and build logs will still be printed."),
        ("--warm-up", "Warm up the Rust environment and exit (for CI/Docker)"),
        ("-h, --help", "Show this help message"),
    ];

    // Find the longest option string for alignment
    let max_option_len = options.iter()
        .map(|(opt, _)| opt.len())
        .max()
        .unwrap_or(0);

    // Build the help message with aligned options
    let mut help_msg = String::from("mcap-videoify - Convert MCAP files containing image data to compressed video\n\n");
    help_msg.push_str("Usage:\n");
    help_msg.push_str("  mcap-videoify [OPTIONS]\n\n");
    help_msg.push_str("Options:\n");

    for (opt, desc) in options {
        help_msg.push_str(&format!("  {:<width$}  {}\n", opt, desc, width = max_option_len));
    }

    help_msg.push_str("\nDescription:\n");
    help_msg.push_str("  This tool processes MCAP files containing image data and converts them to\n");
    help_msg.push_str("  compressed H.264 video streams. It preserves the original message timing\n");
    help_msg.push_str("  and metadata while significantly reducing file size through video compression.");

    help_msg
}


fn print_help() {
    println!("{}", get_help_msg());
}


fn read_it(output_path: &str) -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut input_path = None;
    let mut output_path = output_path.to_string();
    let mut silent = false;
    let mut warmup = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" | "-i" => {
                if i + 1 < args.len() {
                    input_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    anyhow::bail!("Missing value for --input/-i argument");
                }
            }
            "--output" | "-o" => {
                if i + 1 < args.len() {
                    output_path = args[i + 1].clone();
                    i += 2;
                } else {
                    anyhow::bail!("Missing value for --output/-o argument");
                }
            }
            "--silent" => {
                silent = true;
                i += 1;
            }
            "--warm-up" => {
                warmup = true;
                i += 1;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                anyhow::bail!("Unexpected argument: {}. \n\n {}", args[i], get_help_msg());
            }
        }
    }

    // Warm up sequence. Intended for CI & Docker builds.
    if warmup {
        println!("mcap-videoify and underlying rust environment has been warmed up.");
        std::process::exit(0);
    }

    let input_path = input_path.ok_or_else(|| anyhow::anyhow!("No input file specified. Use --input/-i to specify input file"))?;
    let mapped = map_mcap(&input_path)?;

    let mut set = FileDescriptorSet::new();
    set.file
        .push(foxglove::CompressedVideo::file_descriptor().proto().clone());
    set.file.push(
        ::protobuf::well_known_types::timestamp::file_descriptor()
            .proto()
            .clone(),
    );

    let cow = Cow::from(set.write_to_bytes().unwrap());

    let compressed_video_schema = mcap::Schema {
        name: "foxglove.CompressedVideo".to_string(),
        encoding: "protobuf".to_string(),
        data: cow.clone(),
    };

    // Map of topic -> channel for the topic
    let mut topic_channels: HashMap<String, mcap::Channel> = HashMap::new();

    let mut encoders_by_topic: HashMap<String, Encoder> = HashMap::new();

    let mut video_mcap = mcap::Writer::new(BufWriter::new(
        File::create(&output_path).unwrap(),
    ))
    .unwrap();

    for message in mcap::MessageStream::new(&mapped)? {
        let full_message = message.unwrap();
        let schema = full_message.channel.schema.as_ref().unwrap().clone();

        // For other messages, write them as-is
        if schema.name.ne("foxglove.CompressedImage") || schema.encoding.ne("protobuf") {
            if !silent {
                println!("Leaving message as-is: {:?}", schema.name);
            }
            // Write the message as-is to the output MCAP
            video_mcap.write(&full_message).unwrap();
            continue;
        }

        let set_proto = FileDescriptorSet::parse_from_bytes(&schema.data)?;
        let descriptors = FileDescriptor::new_dynamic_fds(set_proto.file, &[]).unwrap();

        // fixme - why index 1?
        let msg = descriptors[1]
            .message_by_full_name(".foxglove.CompressedImage")
            .unwrap();

        let parsed = msg.parse_from_bytes(&full_message.data)?;

        // Only print the message if not silent
        if !silent {
            println!("{:?}", msg);
        }

        let timestamp = msg
            .field_by_name("timestamp")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let frame_id = msg
            .field_by_name("frame_id")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let data = msg
            .field_by_name("data")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let reader = ImageReader::new(Cursor::new(data.to_bytes().unwrap()))
            .with_guessed_format()
            .expect("Cursor io never fails");

        let img = reader.decode()?;

        let rgb8 = &img.to_rgb8();

        let width = usize::try_from(rgb8.width()).unwrap();
        let height = usize::try_from(rgb8.height()).unwrap();

        let topic = std::format!("{topic}_video", topic = full_message.channel.topic);

        let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(||{
            // fixme - command line argument for bitrate
            let config =
                EncoderConfig::new(rgb8.width(), rgb8.height()).set_bitrate_bps(10_000_000);
            
            return Encoder::with_config(config).unwrap();
        });
         
        let yuv = YUVBuffer::with_rgb(width, height, &rgb8);
        let bitstream = encoder.encode(&yuv).unwrap();

        let mut out_msg = foxglove::CompressedVideo::CompressedVideo::new();

        let bytes = timestamp
            .to_message()
            .unwrap()
            .write_to_bytes_dyn()
            .unwrap();
        let time =
            protobuf::well_known_types::timestamp::Timestamp::parse_from_bytes(bytes.as_slice())
                .unwrap();
        out_msg.timestamp.mut_or_insert_default().seconds = time.seconds;
        out_msg.timestamp.mut_or_insert_default().nanos = time.nanos;

        out_msg.frame_id = frame_id.to_string();
        out_msg.format = "h264".to_string();
        out_msg.data = bitstream.to_vec();

        let out_bytes: Vec<u8> = out_msg.write_to_bytes().unwrap();

        let channel = topic_channels.entry(topic.clone()).or_insert_with_key(|key| {
            let new_channel = mcap::Channel {
                schema: Some(Arc::new(compressed_video_schema.to_owned())),
                topic: key.to_string(),
                message_encoding: "protobuf".to_string(),
                metadata: std::collections::BTreeMap::new(),
            };

            video_mcap
                .add_channel(&new_channel)
                .expect("Couldn't write channel");

            return new_channel;
        });

        let message = mcap::Message {
            channel: Arc::new(channel.to_owned()),
            data: Cow::from(out_bytes),
            log_time: full_message.log_time,
            publish_time: full_message.publish_time,
            sequence: full_message.sequence,
        };

        // fixme - why would out_bytes be 0? if the frame did not change?
        if out_msg.data.len() > 0 {
            video_mcap.write(&message).unwrap();
        }
    }

    video_mcap.finish().unwrap();
    Ok(())
}

fn main() {
    if let Err(e) = read_it("compressed_video.mcap") {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
