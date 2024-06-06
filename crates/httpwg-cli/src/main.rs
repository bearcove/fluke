use std::{collections::HashMap, future::Future, pin::Pin, rc::Rc, time::Duration};

use fluke_buffet::{net::TcpStream, IntoHalves};
use httpwg::{rfc9113, Config, Conn};
use tracing::Level;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

fn main() {
    setup_tracing_and_error_reporting();

    let cat = catalog::<fluke_buffet::net::TcpStream>();

    // filter is set to the first argument, if there is a first argument
    let filter = std::env::args().nth(1);

    fluke_buffet::start(async move {
        // now run it! establish a new TCP connection for every case, and run the test.
        let addr = "localhost:8000";
        let conf = Rc::new(Config {
            timeout: Duration::from_secs(3),
            ..Default::default()
        });

        let local_set = tokio::task::LocalSet::new();

        for (rfc, sections) in cat {
            for (section, tests) in sections {
                for (test, boxed_test) in tests {
                    let test_name = format!("{rfc} :: {section} :: {test}");
                    if let Some(filter) = &filter {
                        if !test_name.contains(filter) {
                            println!("Skipping test: {}", test_name);
                            continue;
                        }
                    }

                    let stream =
                        tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(addr))
                            .await
                            .unwrap()
                            .unwrap();
                    let conn = Conn::new(conf.clone(), stream);
                    let test = async move {
                        println!("🔷 Running test: {}", test_name);
                        boxed_test(conn).await.unwrap();
                        println!("✅ Test passed: {}", test_name);
                    };
                    local_set.spawn_local(test);
                }
            }
        }

        local_set.await;
    });
}

type Catalog<IO> =
    HashMap<&'static str, HashMap<&'static str, HashMap<&'static str, BoxedTest<IO>>>>;

#[allow(unused)]
fn print_catalog<IO: IntoHalves>(cat: &Catalog<IO>) {
    for (rfc, sections) in cat {
        println!("📕 {}", rfc);
        for (section, tests) in sections {
            println!("  🔷 {}", section);
            for test in tests.keys() {
                println!("    📄 {}", test);
            }
        }
    }
}

fn setup_tracing_and_error_reporting() {
    color_eyre::install().unwrap();

    let targets = if let Ok(rust_log) = std::env::var("RUST_LOG") {
        rust_log.parse::<Targets>().unwrap()
    } else {
        Targets::new()
            .with_default(Level::INFO)
            .with_target("fluke", Level::DEBUG)
            .with_target("httpwg", Level::DEBUG)
            .with_target("want", Level::INFO)
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_file(false)
        .with_line_number(false)
        .without_time();

    tracing_subscriber::registry()
        .with(targets)
        .with(fmt_layer)
        .init();
}
type BoxedTest<IO> = Box<dyn Fn(Conn<IO>) -> Pin<Box<dyn Future<Output = eyre::Result<()>>>>>;

pub fn catalog<IO: IntoHalves>(
) -> HashMap<&'static str, HashMap<&'static str, HashMap<&'static str, BoxedTest<IO>>>> {
    let mut rfcs: HashMap<
        &'static str,
        HashMap<&'static str, HashMap<&'static str, BoxedTest<IO>>>,
    > = Default::default();

    {
        let mut sections: HashMap<&'static str, _> = Default::default();

        {
            use rfc9113::_8_expressing_http_semantics_in_http2 as s;
            let mut section8: HashMap<&'static str, BoxedTest<IO>> = Default::default();
            section8.insert(
                "client sends push promise frame",
                Box::new(|conn: Conn<IO>| Box::pin(s::client_sends_push_promise_frame(conn))),
            );
            section8.insert(
                "sends connect with scheme",
                Box::new(|conn: Conn<IO>| Box::pin(s::sends_connect_with_scheme(conn))),
            );
            section8.insert(
                "sends connect with path",
                Box::new(|conn: Conn<IO>| Box::pin(s::sends_connect_with_path(conn))),
            );

            sections.insert("Section 8: Expressing HTTP Semantics in HTTP/2", section8);
        }

        rfcs.insert("RFC 9113", sections);
    }

    rfcs
}
