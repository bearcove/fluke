//! Section 8: Expressing HTTP Semantics in HTTP/2

use std::io::Write;

use fluke_buffet::IntoHalves;
use fluke_h2_parse::{pack_bit_and_u31, FrameType, HeadersFlags, StreamId};

use crate::{Conn, ErrorC, FrameT, Headers};

//---- Section 8.1: HTTP Message Framing

// An endpoint that receives a HEADERS frame without the
// END_STREAM flag set after receiving a final (non-informational)
// status code MUST treat the corresponding request or response
// as malformed (Section 8.1.2.6).
pub async fn sends_second_headers_frame_without_end_stream<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    let stream_id = StreamId(1);
    conn.handshake().await?;

    let headers_fragment = conn.encode_headers(&conn.common_headers("POST"))?;
    conn.write_headers(stream_id, HeadersFlags::EndHeaders, headers_fragment)
        .await?;
    conn.write_data(stream_id, false, b"test").await?;

    let mut trailers = Headers::default();
    trailers.append("x-test", "ok");
    let trailers_fragment = conn.encode_headers(&trailers)?;
    conn.write_headers(stream_id, HeadersFlags::EndHeaders, trailers_fragment)
        .await?;

    conn.verify_stream_error(ErrorC::ProtocolError).await?;

    Ok(())
}

//--- Section 8.1.1: Malformed Messages

// A request or response that includes message content can include a
// content-length header field. A request or response is also malformed if the
// value of a content-length header field does not equal the sum of the DATA
// frame payload lengths that form the content, unless the message is defined as
// having no content. For example, 204 or 304 responses contain no content, as
// does the response to a HEAD request. A response that is defined to have no
// content, as described in Section 6.4.1 of [HTTP], MAY have a non-zero
// content-length header field, even though no content is included in DATA
// frames.
//
// Intermediaries that process HTTP requests or responses (i.e., any
// intermediary not acting as a tunnel) MUST NOT forward a malformed request or
// response. Malformed requests or responses that are detected MUST be treated
// as a stream error (Section 5.4.2) of type PROTOCOL_ERROR.

pub async fn sends_headers_frame_with_incorrect_content_length_single_data_frame<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    let stream_id = StreamId(1);
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("content-length", "10");
    let block_fragment = conn.encode_headers(&headers);
    conn.write_headers(stream_id, HeadersFlags::EndHeaders, block_fragment?)
        .await?;
    conn.write_data(stream_id, true, b"test").await?;

    conn.verify_stream_error(ErrorC::ProtocolError).await?;

    Ok(())
}

pub async fn sends_headers_frame_with_incorrect_content_length_multiple_data_frames<
    IO: IntoHalves,
>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    let stream_id = StreamId(1);
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("content-length", "10");
    let block_fragment = conn.encode_headers(&headers);
    conn.write_headers(stream_id, HeadersFlags::EndHeaders, block_fragment?)
        .await?;
    conn.write_data(stream_id, false, b"te").await?;
    conn.write_data(stream_id, false, b"st").await?;
    conn.write_data(stream_id, true, b"ing").await?;

    conn.verify_stream_error(ErrorC::ProtocolError).await?;

    Ok(())
}

//--- Section 8.2.1: Field Validity

/// A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a,
/// or 0x7f-0xff (all ranges inclusive). This specifically excludes all
/// non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A'
/// to 'Z', ASCII 0x41 to 0x5a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_uppercase_field_name<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("UPPERCASE", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a,
/// or 0x7f-0xff (all ranges inclusive). This specifically excludes all
/// non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A'
/// to 'Z', ASCII 0x41 to 0x5a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_space_in_field_name<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("space force", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a,
/// or 0x7f-0xff (all ranges inclusive). This specifically excludes all
/// non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A'
/// to 'Z', ASCII 0x41 to 0x5a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_non_visible_ascii<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("\x01invalid", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a,
/// or 0x7f-0xff (all ranges inclusive). This specifically excludes all
/// non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A'
/// to 'Z', ASCII 0x41 to 0x5a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_del_character<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("\x7Finvalid", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a,
/// or 0x7f-0xff (all ranges inclusive). This specifically excludes all
/// non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A'
/// to 'Z', ASCII 0x41 to 0x5a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_non_ascii_character<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("inválid", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// With the exception of pseudo-header fields (Section 8.3), which have a name
/// that starts with a single colon, field names MUST NOT include a colon (ASCII
/// COLON, 0x3a).
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_colon_in_field_name<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid:field", "oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field value MUST NOT contain the zero value (ASCII NUL, 0x00), line feed
/// (ASCII LF, 0x0a), or carriage return (ASCII CR, 0x0d) at any position.
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_lf_in_field_value<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid-value", "oh\nno");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field value MUST NOT contain the zero value (ASCII NUL, 0x00), line feed
/// (ASCII LF, 0x0a), or carriage return (ASCII CR, 0x0d) at any position.
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_cr_in_field_value<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid-value", "oh\rno");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field value MUST NOT contain the zero value (ASCII NUL, 0x00), line feed
/// (ASCII LF, 0x0a), or carriage return (ASCII CR, 0x0d) at any position.
///
/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_nul_in_field_value<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid-value", "oh\0no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field value MUST NOT start or end with an ASCII whitespace character
/// (ASCII SP or HTAB, 0x20 or 0x09).

/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_leading_space_in_field_value<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid-value", " oh no");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A field value MUST NOT start or end with an ASCII whitespace character
/// (ASCII SP or HTAB, 0x20 or 0x09).

/// When a request message violates one of these requirements, an implementation
/// SHOULD generate a 400 (Bad Request) status code (see Section 15.5.1 of
/// [HTTP]), unless a more suitable status code is defined or the status code
/// cannot be sent (e.g., because the error occurs in a trailer field).
pub async fn sends_headers_frame_with_trailing_tab_in_field_value<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("invalid-value", "oh no\t");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

//---- Section 8.2.2: Connection-Specific Header Fields

/// HTTP/2 does not use the Connection header field (Section 7.6.1 of [HTTP]) to
/// indicate connection-specific header fields; in this protocol,
/// connection-specific metadata is conveyed by other means. An endpoint MUST
/// NOT generate an HTTP/2 message containing connection-specific header fields.
/// This includes the Connection header field and those listed as having
/// connection-specific semantics in Section 7.6.1 of [HTTP] (that is,
/// Proxy-Connection, Keep-Alive, Transfer-Encoding, and Upgrade). Any message
/// containing connection-specific header fields MUST be treated as malformed
/// (Section 8.1.1).
pub async fn sends_headers_frame_with_connection_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("connection", "keep-alive");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// HTTP/2 does not use the Connection header field (Section 7.6.1 of [HTTP]) to
/// indicate connection-specific header fields; in this protocol,
/// connection-specific metadata is conveyed by other means. An endpoint MUST
/// NOT generate an HTTP/2 message containing connection-specific header fields.
///
/// This includes the Connection header field and those listed as having
/// connection-specific semantics in Section 7.6.1 of [HTTP] (that is,
/// Proxy-Connection, Keep-Alive, Transfer-Encoding, and Upgrade). Any message
/// containing connection-specific header fields MUST be treated as malformed
/// (Section 8.1.1).
pub async fn sends_headers_frame_with_proxy_connection_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("proxy-connection", "keep-alive");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// HTTP/2 does not use the Connection header field (Section 7.6.1 of [HTTP]) to
/// indicate connection-specific header fields; in this protocol,
/// connection-specific metadata is conveyed by other means. An endpoint MUST
/// NOT generate an HTTP/2 message containing connection-specific header fields.
///
/// This includes the Connection header field and those listed as having
/// connection-specific semantics in Section 7.6.1 of [HTTP] (that is,
/// Proxy-Connection, Keep-Alive, Transfer-Encoding, and Upgrade). Any message
/// containing connection-specific header fields MUST be treated as malformed
/// (Section 8.1.1).
pub async fn sends_headers_frame_with_keep_alive_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("keep-alive", "timeout=5");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// HTTP/2 does not use the Connection header field (Section 7.6.1 of [HTTP]) to
/// indicate connection-specific header fields; in this protocol,
/// connection-specific metadata is conveyed by other means. An endpoint MUST
/// NOT generate an HTTP/2 message containing connection-specific header fields.
///
/// This includes the Connection header field and those listed as having
/// connection-specific semantics in Section 7.6.1 of [HTTP] (that is,
/// Proxy-Connection, Keep-Alive, Transfer-Encoding, and Upgrade). Any message
/// containing connection-specific header fields MUST be treated as malformed
/// (Section 8.1.1).
pub async fn sends_headers_frame_with_transfer_encoding_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("transfer-encoding", "chunked");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// HTTP/2 does not use the Connection header field (Section 7.6.1 of [HTTP]) to
/// indicate connection-specific header fields; in this protocol,
/// connection-specific metadata is conveyed by other means. An endpoint MUST
/// NOT generate an HTTP/2 message containing connection-specific header fields.
///
/// This includes the Connection header field and those listed as having
/// connection-specific semantics in Section 7.6.1 of [HTTP] (that is,
/// Proxy-Connection, Keep-Alive, Transfer-Encoding, and Upgrade). Any message
/// containing connection-specific header fields MUST be treated as malformed
/// (Section 8.1.1).
pub async fn sends_headers_frame_with_upgrade_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("upgrade", "h2c");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// The only exception to this is the TE header field, which MAY be present in
/// an HTTP/2 request; when it is, it MUST NOT contain any value other than
/// "trailers".
pub async fn sends_headers_frame_with_te_trailers<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("te", "trailers");
    conn.send_req_and_expect_status(StreamId(1), &headers, 200)
        .await?;

    Ok(())
}

/// The only exception to this is the TE header field, which MAY be present in
/// an HTTP/2 request; when it is, it MUST NOT contain any value other than
/// "trailers".
pub async fn sends_headers_frame_with_te_not_trailers<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append("te", "not-trailers");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

//---- Section 8.2.3: Compressing the Cookie Header Field

// That can't really be tested without controlling both sides of the
// connection, so, not suited for this test suite.

//---- Section 8.3: HTTP Control Data

/// [...] pseudo-header fields defined for responses MUST NOT appear in requests
/// [...] Endpoints MUST treat a request or response that contains undefined or
/// invalid pseudo-header fields as malformed (Section 8.1.1).
pub async fn sends_headers_frame_with_response_pseudo_header<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.append(":status", "200");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// [...] Pseudo-header fields MUST NOT appear in a trailer section. Endpoints
/// MUST treat a request or response that contains undefined or invalid
/// pseudo-header fields as malformed (Section 8.1.1).
pub async fn sends_headers_frame_with_pseudo_header_in_trailer<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    let stream_id = StreamId(1);
    conn.handshake().await?;

    let headers_fragment = conn.encode_headers(&conn.common_headers("POST"))?;
    conn.write_headers(stream_id, HeadersFlags::EndHeaders, headers_fragment)
        .await?;
    conn.write_data(stream_id, false, b"test").await?;

    let mut trailers = Headers::default();
    trailers.append(":method", "POST");
    let trailers_fragment = conn.encode_headers(&trailers)?;
    conn.write_headers(
        stream_id,
        HeadersFlags::EndHeaders | HeadersFlags::EndStream,
        trailers_fragment,
    )
    .await?;

    conn.verify_stream_error(ErrorC::ProtocolError).await?;

    Ok(())
}

/// The same pseudo-header field name MUST NOT appear more than once in a field
/// block. A field block for an HTTP request or response that contains a
/// repeated pseudo-header field name MUST be treated as malformed (Section
/// 8.1.1).
pub async fn sends_headers_frame_with_duplicate_pseudo_headers<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.prepend(":method", "POST");
    headers.prepend(":method", "POST");

    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// A server SHOULD treat a request as malformed if it contains a Host header
/// field that identifies an entity that differs from the entity in the
/// ":authority" pseudo-header field. The values of fields need to be normalized
/// to compare them (see Section 6.2 of [RFC3986]). An origin server can apply
/// any normalization method, whereas other servers MUST perform scheme-based
/// normalization (see Section 6.2.3 of [RFC3986]) of the two fields.
///
/// cf. <https://www.rfc-editor.org/rfc/rfc3986.html#section-6.2.3>
pub async fn sends_headers_frame_with_mismatched_host_authority<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.replace(":authority", conn.config.host.clone().into_bytes());
    headers.append(
        "host",
        format!("{}.different", conn.config.host).into_bytes(),
    );
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

//---
// Note: URI normalization is a can of worms, see <https://www.rfc-editor.org/rfc/rfc3986.html#section-6.2>
// It seems hard to test it in a way that most HTTP/2 implementations would
// pass.
//---

/// This pseudo-header field MUST NOT be empty for "http" or "https" URIs;
/// "http" or "https" URIs that do not contain a path component MUST include a
/// value of '/'. The exceptions to this rule are:
///
/// an OPTIONS request for an "http" or "https" URI that does not include a path
/// component; these MUST include a ":path" pseudo-header field with a value of
/// '*' (see Section 7.1 of [HTTP]). CONNECT requests (Section 8.5), where the
/// ":path" pseudo-header field is omitted.
pub async fn sends_headers_frame_with_empty_path_component<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.replace(":path", "");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// All HTTP/2 requests MUST include exactly one valid value for the ":method",
/// ":scheme", and ":path" pseudo-header fields, unless they are CONNECT
/// requests (Section 8.5). An HTTP request that omits mandatory pseudo-header
/// fields is malformed (Section 8.1.1).

pub async fn sends_headers_frame_without_method<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.remove(&":method".into());
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

pub async fn sends_headers_frame_without_scheme<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.remove(&":scheme".into());
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

pub async fn sends_headers_frame_without_path<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = conn.common_headers("POST");
    headers.remove(&":path".into());
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

//---- Section 8.3.2: Response Pseudo-Header Fields

pub async fn sends_headers_frame_without_status<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let block_fragment = conn.encode_headers(&conn.common_headers("POST"))?;
    conn.write_frame(
        FrameType::Headers(HeadersFlags::EndStream | HeadersFlags::EndHeaders)
            .into_frame(StreamId(1)),
        block_fragment,
    )
    .await?;

    // wait for the response
    let (frame, payload) = conn.wait_for_frame(FrameT::Headers).await.unwrap();
    assert!(frame.is_end_headers(), "the test makes that assumption");
    let headers = conn.decode_headers(payload.into())?;

    let mut found_status = false;
    for (name, _) in headers.iter() {
        if name == b":status" {
            found_status = true;
        } else {
            assert!(
                !name.starts_with(b":"),
                "no header name should start with ':'"
            );
        }
    }
    assert!(found_status, "the :status pseudo-header must be present");

    Ok(())
}

//--- Section 8.4: Server Push

// Server push is discouraged now:
//
// In practice, server push is difficult to use effectively, because it requires
// the server to correctly anticipate the additional requests the client will
// make, taking into account factors such as caching, content negotiation, and
// user behavior. Errors in prediction can lead to performance degradation, due
// to the opportunity cost that the additional data on the wire represents. In
// particular, pushing any significant amount of data can cause contention
// issues with responses that are more important.

/// A client cannot push. Thus, servers MUST treat the receipt of a PUSH_PROMISE
/// frame as a connection error (Section 5.4.1) of type PROTOCOL_ERROR. A server
/// cannot set the SETTINGS_ENABLE_PUSH setting to a value other than 0 (see
/// Section 6.5.2).
pub async fn client_sends_push_promise_frame<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let stream_id = StreamId(1);
    let promised_stream_id = StreamId(2);

    let mut headers = Headers::default();
    headers.append(":status", "200");
    let block_fragment = conn.encode_headers(&headers)?;
    let payload = conn
        .scratch
        .put_to_roll(block_fragment.len() + 4, |mut s| {
            s.write_all(&pack_bit_and_u31(0, promised_stream_id.0))?;
            s.write_all(&block_fragment)?;
            Ok(())
        })?;
    conn.write_frame(FrameType::PushPromise.into_frame(stream_id), payload)
        .await?;

    conn.verify_connection_error(ErrorC::ProtocolError).await?;

    Ok(())
}

//---- Section 8.5: The CONNECT Method

/// The CONNECT method (Section 9.3.6 of [HTTP]) is used to convert an HTTP
/// connection into a tunnel to a remote host. CONNECT is primarily used with
/// HTTP proxies to establish a TLS session with an origin server for the
/// purposes of interacting with "https" resources.
///
/// In HTTP/2, the CONNECT method establishes a tunnel over a single HTTP/2
/// stream to a remote host, rather than converting the entire connection to a
/// tunnel. A CONNECT header section is constructed as defined in Section 8.3.1
/// ("Request Pseudo-Header Fields"), with a few differences. Specifically:
///
/// The ":method" pseudo-header field is set to CONNECT.
/// The ":scheme" and ":path" pseudo-header fields MUST be omitted.
/// The ":authority" pseudo-header field contains the host and port to connect
/// to (equivalent to the authority-form of the request-target of CONNECT
/// requests; see Section 3.2.3 of [HTTP/1.1]).

pub async fn sends_connect_with_scheme<IO: IntoHalves>(mut conn: Conn<IO>) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = Headers::default();
    headers.append(":method", "CONNECT");
    headers.append(":scheme", "https");
    headers.append(":authority", "example.com:443");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

pub async fn sends_connect_with_path<IO: IntoHalves>(mut conn: Conn<IO>) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = Headers::default();
    headers.append(":method", "CONNECT");
    headers.append(":path", "/");
    headers.append(":authority", "example.com:443");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

pub async fn sends_connect_without_authority<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = Headers::default();
    headers.append(":method", "CONNECT");
    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

/// All pseudo-header fields MUST appear in a field block before all regular
/// field lines (RFC 9113, section 8.3)
pub async fn sends_headers_frame_with_pseudo_headers_after_regular_headers<IO: IntoHalves>(
    mut conn: Conn<IO>,
) -> eyre::Result<()> {
    conn.handshake().await?;

    let mut headers = Headers::default();
    headers.append(":method", "POST");
    headers.append(":path", "/");
    headers.append("content-type", "application/json");
    headers.append(":authority", "example.com");

    conn.send_req_and_expect_stream_rst(StreamId(1), &headers)
        .await?;

    Ok(())
}

//---- Section 8.6: The Upgrade Header Field

// (No tests for this section: 101 is not a thing in HTTP/2)

//---- Section 8.7: Request Reliability

// HTTP/2 provides two mechanisms for providing a guarantee to a client that a
// request has not been processed:
//
// The GOAWAY frame indicates the highest stream number that might have been
// processed. Requests on streams with higher numbers are therefore guaranteed
// to be safe to retry. The REFUSED_STREAM error code can be included in a
// RST_STREAM frame to indicate that the stream is being closed prior to any
// processing having occurred. Any request that was sent on the reset stream
// can be safely retried. Requests that have not been processed have not
// failed; clients MAY automatically retry them, even those with non-idempotent
// methods.
//
// A server MUST NOT indicate that a stream has not been processed unless it
// can guarantee that fact. If frames that are on a stream are passed to the
// application layer for any stream, then REFUSED_STREAM MUST NOT be used for
// that stream, and a GOAWAY frame MUST include a stream identifier that is
// greater than or equal to the given stream identifier.
//
// ------------
//
// Note: this feels impossible to test while only controlling the client,
// because: we cannot force the server to send us REFUSED_STREAM (short of
// opening potentially hundreds of streams — and even then, it might just get
// overloaded rather than starting to refuse streams).
//
// We can't force the server to send us GOAWAY either, because it's allowed to
// close the connection without sending one, cf. Section 6.8:
//
//   > An endpoint might choose to close a connection without sending a GOAWAY
//   > for misbehaving peers.
//
// Testing the server would only be possible if it had a
// `/.well-known/initiate-graceful-shutdown` endpoint, and additionally if we
// could check what requests were _actually_ partially processed (most likely
// forwarded to an origin server).
//
// This feels like a good integration test for a proxy server, but it's not
// suitable for this suite (no pun intended).
