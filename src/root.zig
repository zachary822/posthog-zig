//! A PostHog client that sends events to the capture v1 API
//! (`POST /i/v1/analytics/events`), the same protocol as `posthog-python`
//! with `capture_mode="v1"`.
//!
//!     const client = try posthog.Client.create(gpa, io, .{ .api_key = "phc_..." });
//!     defer client.destroy();
//!     _ = try client.capture("user_signed_up", "user123", .{ .plan = "free" });
//!
//! `capture` encodes the event as JSON into the queue of the client, and
//! returns its uuid. A thread of the client sends the queue in batches. The
//! server gives a result for each event: the client sends again only the
//! events that the server marks `retry`, with exponential backoff.
//! `destroy` sends the events in the queue before it returns.
//!
//! Properties are any value that `std.json.Stringify` writes as a JSON
//! object: a struct, a `std.json.Value` object or a `std.json.ArrayHashMap`.
//! `.{}` or `null` sends no properties.

const std = @import("std");

const log = std.log.scoped(.posthog);

pub const version = "0.1.0";
/// The `User-Agent` and `PostHog-Sdk-Info` headers. The server sets the
/// `$lib` and `$lib_version` properties from it.
pub const sdk_info = "posthog-zig/" ++ version;

pub const us_host = "https://us.i.posthog.com";
pub const eu_host = "https://eu.i.posthog.com";

const capture_path = "/i/v1/analytics/events";

/// A UUID as text, for example `"0b7f2d4e-1c3a-4f5b-9d6e-7a8b9c0d1e2f"`.
pub const Uuid = [36]u8;

pub const Options = struct {
    /// The project API key (`phc_...`).
    api_key: []const u8,
    /// The ingestion host, without a path.
    host: []const u8 = us_host,
    /// Seconds between two sends, if the queue does not get full first.
    flush_interval_s: u32 = 5,
    /// Seconds for one request. A request that does not finish in this time
    /// is canceled, and counts as a failed attempt.
    send_timeout_s: u32 = 15,
    /// Attempts after the first one, for events that the server marks
    /// `retry`, transient statuses and network errors.
    max_retries: u32 = 3,
    /// Sets `historical_migration` on each batch, for imports of old events.
    historical_migration: bool = false,
    /// The compression of the request bodies.
    compression: Compression = .none,
};

pub const Compression = enum {
    none,
    gzip,
    /// zlib (RFC 1950), which the server decodes for
    /// `Content-Encoding: deflate`. Raw deflate is not correct.
    deflate,

    fn container(c: Compression) ?std.compress.flate.Container {
        return switch (c) {
            .none => null,
            .gzip => .gzip,
            .deflate => .zlib,
        };
    }
};

pub const CaptureError = error{
    OutOfMemory,
    /// `properties` is not written as a JSON object.
    PropertiesNotObject,
    /// The JSON of the event is larger than `max_event_size`.
    EventTooLarge,
    /// The queue has `max_queue_size` bytes: the server is not available.
    QueueFull,
    /// `alias` or `groupIdentify` got an empty id.
    MissingId,
};

/// The server drops events that are larger.
pub const max_event_size = 900 * 1024;
/// `capture` returns `error.QueueFull` when the queue has this number of
/// bytes.
pub const max_queue_size = 16 * 1024 * 1024;
/// Maximums for one request.
const max_batch_events = 100;
const max_batch_size = 5 * 1024 * 1024;
/// The maximum backoff, and the maximum `Retry-After` that the client obeys.
const max_backoff_s = 30;

pub const Client = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    /// `host` + `capture_path`.
    url: []const u8,
    /// `"Bearer " ++ api_key`.
    authorization: []const u8,
    options: Options,

    mutex: std.Io.Mutex = .init,
    /// Signaled when the queue has `max_batch_events` events, and by
    /// `destroy`.
    wake: std.Io.Condition = .init,
    /// The JSON of each queued event. The client owns the slices.
    queue: std.ArrayList([]u8) = .empty,
    queue_size: usize = 0,
    stopping: bool = false,
    thread: ?std.Thread = null,
    /// The last request did not succeed: log the next result only when it
    /// changes, so a server that is down does not fill the log.
    failing: bool = false,

    /// Makes a client and starts its thread.
    pub fn create(gpa: std.mem.Allocator, io: std.Io, options: Options) !*Client {
        const self = try init(gpa, io, options);
        errdefer self.destroy();
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }

    /// Makes a client without a thread (for the tests).
    fn init(gpa: std.mem.Allocator, io: std.Io, options: Options) !*Client {
        if (options.api_key.len == 0) return error.MissingApiKey;
        const host = std.mem.trimEnd(u8, std.mem.trim(u8, options.host, " \t"), "/");
        const url = try std.fmt.allocPrint(gpa, "{s}{s}", .{ host, capture_path });
        errdefer gpa.free(url);
        _ = std.Uri.parse(url) catch return error.InvalidHost;
        const authorization = try std.fmt.allocPrint(gpa, "Bearer {s}", .{options.api_key});
        errdefer gpa.free(authorization);

        const self = try gpa.create(Client);
        self.* = .{
            .gpa = gpa,
            .io = io,
            .url = url,
            .authorization = authorization,
            .options = options,
        };
        return self;
    }

    /// Sends the events in the queue, stops the thread and frees the
    /// client. Call it after the last `capture`. A server that is not
    /// available makes it wait for the attempts of one batch, but not for
    /// the backoff between them.
    pub fn destroy(self: *Client) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        self.stopping = true;
        self.mutex.unlock(io);
        self.wake.signal(io);
        if (self.thread) |t| t.join();

        for (self.queue.items) |event| self.gpa.free(event);
        self.queue.deinit(self.gpa);
        self.gpa.free(self.authorization);
        self.gpa.free(self.url);
        self.gpa.destroy(self);
    }

    /// Captures an event. A null `distinct_id` makes a personless event:
    /// a random distinct id, and no person profile.
    pub fn capture(self: *Client, event: []const u8, distinct_id: ?[]const u8, properties: anytype) CaptureError!Uuid {
        return self.enqueue(event, distinct_id, distinct_id == null, properties);
    }

    /// Sets properties on the person profile of `distinct_id`.
    pub fn set(self: *Client, distinct_id: []const u8, properties: anytype) CaptureError!Uuid {
        if (distinct_id.len == 0) return error.MissingId;
        return self.enqueue("$set", distinct_id, false, .{ .@"$set" = properties });
    }

    /// Sets properties on the person profile of `distinct_id`, only if they
    /// do not have a value.
    pub fn setOnce(self: *Client, distinct_id: []const u8, properties: anytype) CaptureError!Uuid {
        if (distinct_id.len == 0) return error.MissingId;
        return self.enqueue("$set_once", distinct_id, false, .{ .@"$set_once" = properties });
    }

    /// Makes `distinct_id` an alias of `previous_id`: both ids are then the
    /// same person.
    pub fn alias(self: *Client, previous_id: []const u8, distinct_id: []const u8) CaptureError!Uuid {
        if (previous_id.len == 0 or distinct_id.len == 0) return error.MissingId;
        return self.enqueue("$create_alias", previous_id, false, .{
            .distinct_id = previous_id,
            .alias = distinct_id,
        });
    }

    /// Sets properties on a group, for example `("company", "acme", ...)`.
    /// A null `distinct_id` is a random id, as in `posthog-python`.
    pub fn groupIdentify(self: *Client, group_type: []const u8, group_key: []const u8, distinct_id: ?[]const u8, properties: anytype) CaptureError!Uuid {
        if (group_type.len == 0 or group_key.len == 0) return error.MissingId;
        return self.enqueue("$groupidentify", distinct_id, false, .{
            .@"$group_type" = group_type,
            .@"$group_key" = group_key,
            .@"$group_set" = properties,
        });
    }

    fn enqueue(self: *Client, event: []const u8, distinct_id: ?[]const u8, personless: bool, properties: anytype) CaptureError!Uuid {
        const io = self.io;
        const uuid = newUuid(io);
        var random_id: Uuid = undefined;
        const id = if (distinct_id) |d| d else blk: {
            random_id = newUuid(io);
            break :blk &random_id;
        };
        const now_ns = std.Io.Timestamp.now(io, .real).toNanoseconds();
        const json = try encodeEvent(self.gpa, .{
            .uuid = uuid,
            .event = event,
            .distinct_id = id,
            .timestamp_ns = now_ns,
            .personless = personless,
        }, properties);
        errdefer self.gpa.free(json);

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.queue_size + json.len > max_queue_size) return error.QueueFull;
        try self.queue.append(self.gpa, json);
        self.queue_size += json.len;
        if (self.queue.items.len == max_batch_events) self.wake.signal(io);
        return uuid;
    }

    /// The thread: waits for a full batch or `flush_interval_s`, then sends
    /// the queue. After `destroy`, sends the rest and stops.
    fn run(self: *Client) void {
        const io = self.io;
        var client: std.http.Client = .{ .allocator = self.gpa, .io = io };
        defer client.deinit();
        // The events that are sent. The queue gets new events in the
        // meantime.
        var batch: std.ArrayList([]u8) = .empty;
        defer batch.deinit(self.gpa);
        var body: std.Io.Writer.Allocating = .init(self.gpa);
        defer body.deinit();
        var response: std.Io.Writer.Allocating = .init(self.gpa);
        defer response.deinit();
        var compressor: ?*Compressor = null;
        defer if (compressor) |c| c.destroy();
        if (self.options.compression.container()) |container| {
            compressor = Compressor.create(self.gpa, container) catch blk: {
                log.warn("cannot compress (out of memory): sending without compression", .{});
                break :blk null;
            };
        }

        while (true) {
            self.mutex.lockUncancelable(io);
            if (!self.stopping and self.queue.items.len < max_batch_events) {
                self.wake.waitTimeout(io, &self.mutex, .{ .duration = .{
                    .raw = .fromSeconds(self.options.flush_interval_s),
                    .clock = .awake,
                } }) catch {};
            }
            std.mem.swap(std.ArrayList([]u8), &batch, &self.queue);
            self.queue_size = 0;
            const stopping = self.stopping;
            self.mutex.unlock(io);

            var rest = batch.items;
            while (rest.len > 0) {
                const n = batchLen(rest);
                self.deliver(&client, &body, &response, compressor, rest[0..n]);
                rest = rest[n..];
            }
            batch.clearRetainingCapacity();
            if (stopping) return;
        }
    }

    /// Sends `events` in requests with the same request id, until the
    /// server accepts or drops each event, or the attempts are done. Frees
    /// the events.
    fn deliver(
        self: *Client,
        client: *std.http.Client,
        body: *std.Io.Writer.Allocating,
        response: *std.Io.Writer.Allocating,
        compressor: ?*Compressor,
        events: [][]u8,
    ) void {
        const io = self.io;
        var pending = events;
        defer for (pending) |event| self.gpa.free(event);

        const request_id = newUuid(io);
        var created_at_buf: [timestamp_len]u8 = undefined;
        const created_at = formatTimestamp(&created_at_buf, std.Io.Timestamp.now(io, .real).toNanoseconds());
        var dropped: usize = 0;
        defer if (dropped > 0) log.warn("the server dropped {d} events", .{dropped});

        var attempt: u32 = 1;
        while (true) : (attempt += 1) {
            const last = attempt > self.options.max_retries;
            body.clearRetainingCapacity();
            writeBatch(&body.writer, created_at, pending, self.options.historical_migration) catch {
                log.warn("dropped {d} events (out of memory)", .{pending.len});
                return;
            };
            var payload: Payload = .{ .body = body.written() };
            if (compressor) |c| {
                if (c.compress(body.written())) |compressed| {
                    payload = .{ .body = compressed, .content_encoding = @tagName(self.options.compression) };
                } else |_| {
                    log.warn("cannot compress (out of memory): sending without compression", .{});
                }
            }
            response.clearRetainingCapacity();
            const result = self.post(client, payload, response, &request_id, attempt) catch |err| {
                if (!self.failing) log.warn("cannot send events to {s}: {t}", .{ self.url, err });
                self.failing = true;
                if (last or !self.backoff(attempt, null)) {
                    log.warn("dropped {d} events after {d} attempts", .{ pending.len, attempt });
                    return;
                }
                continue;
            };

            const status = @backingInt(result.status);
            if (result.status.class() != .success) {
                if (!self.failing) log.warn("{s} answered {d}: {s}", .{ self.url, status, response.written() });
                self.failing = true;
                const retryable = switch (status) {
                    408, 500, 502, 503, 504 => true,
                    else => false,
                };
                if (!retryable or last or !self.backoff(attempt, result.retry_after)) {
                    log.warn("dropped {d} events after {d} attempts", .{ pending.len, attempt });
                    return;
                }
                continue;
            }
            if (self.failing) log.info("sending events to {s} again", .{self.url});
            self.failing = false;
            log.debug("request {s} attempt {d}: {d} events, {d}: {s}", .{ &request_id, attempt, pending.len, status, response.written() });

            const retained = self.retainRetries(pending, response.written(), &dropped) catch {
                log.warn("{s} answered {d} with a body that is not a results map", .{ self.url, status });
                return;
            };
            pending = pending[0..retained];
            if (pending.len == 0) return;
            if (last or !self.backoff(attempt, result.retry_after)) {
                log.warn("dropped {d} events after {d} attempts (the server asked for a retry)", .{ pending.len, attempt });
                return;
            }
        }
    }

    /// Moves the events that the server marks `retry` to the start of
    /// `events`, frees the others and returns the number of retries. An
    /// event that is not in `results` is accepted, as in `posthog-python`.
    fn retainRetries(self: *Client, events: [][]u8, response: []const u8, dropped: *usize) !usize {
        const Result = struct {
            result: ?[]const u8 = null,
            details: ?[]const u8 = null,
        };
        const parsed = try std.json.parseFromSlice(struct {
            results: std.json.ArrayHashMap(?Result),
        }, self.gpa, response, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const results = &parsed.value.results.map;

        var n: usize = 0;
        for (events) |event| {
            const result: Result = (results.get(eventUuid(event)) orelse null) orelse .{};
            const r = result.result orelse "";
            if (std.mem.eql(u8, r, "retry")) {
                events[n] = event;
                n += 1;
                continue;
            }
            if (std.mem.eql(u8, r, "drop")) {
                dropped.* += 1;
                log.debug("the server dropped event {s}: {s}", .{ eventUuid(event), result.details orelse "" });
            }
            self.gpa.free(event);
        }
        return n;
    }

    /// Waits before the next attempt: `2^(attempt - 1)` seconds, a minimum
    /// of `retry_after`, and a maximum of `max_backoff_s`. Returns false
    /// without a wait after `destroy`, so that `destroy` does not wait.
    fn backoff(self: *Client, attempt: u32, retry_after: ?u32) bool {
        const io = self.io;
        const exponential = std.math.shl(u64, 1, attempt - 1);
        const seconds = @min(@max(exponential, retry_after orelse 0), max_backoff_s);
        const deadline: std.Io.Clock.Timestamp = .fromNow(io, .{
            .raw = .fromSeconds(@intCast(seconds)),
            .clock = .awake,
        });
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        // `capture` can signal `wake` before the deadline.
        while (!self.stopping) {
            self.wake.waitTimeout(io, &self.mutex, .{ .deadline = deadline }) catch return true;
        }
        return false;
    }

    const PostResult = struct {
        status: std.http.Status,
        retry_after: ?u32,
    };

    /// POSTs `body`, and stops after `send_timeout_s`. `std.http.Client` has
    /// no timeout, so the request runs as a separate task, which is canceled
    /// if it does not finish in time.
    fn post(
        self: *Client,
        client: *std.http.Client,
        payload: Payload,
        response: *std.Io.Writer.Allocating,
        request_id: *const Uuid,
        attempt: u32,
    ) !PostResult {
        const io = self.io;
        var attempt_buf: [10]u8 = undefined;
        var timestamp_buf: [timestamp_len]u8 = undefined;
        var headers = [_]std.http.Header{
            .{ .name = "PostHog-Sdk-Info", .value = sdk_info },
            .{ .name = "PostHog-Attempt", .value = std.fmt.bufPrint(&attempt_buf, "{d}", .{attempt}) catch unreachable },
            .{ .name = "PostHog-Request-Id", .value = request_id },
            .{ .name = "PostHog-Request-Timestamp", .value = formatTimestamp(&timestamp_buf, std.Io.Timestamp.now(io, .real).toNanoseconds()) },
            undefined,
        };
        var header_count: usize = headers.len - 1;
        if (payload.content_encoding) |encoding| {
            headers[header_count] = .{ .name = "Content-Encoding", .value = encoding };
            header_count += 1;
        }

        var done: std.Io.Event = .unset;
        var task = try io.concurrent(postTask, .{ io, client, self.url, self.authorization, headers[0..header_count], payload.body, response, &done });
        done.waitTimeout(io, .{ .duration = .{
            .raw = .fromSeconds(self.options.send_timeout_s),
            .clock = .awake,
        } }) catch {};
        if (!done.isSet()) {
            _ = task.cancel(io) catch {};
            return error.Timeout;
        }
        return task.await(io);
    }

    fn postTask(
        io: std.Io,
        client: *std.http.Client,
        url: []const u8,
        authorization: []const u8,
        headers: []const std.http.Header,
        body: []u8,
        response: *std.Io.Writer.Allocating,
        done: *std.Io.Event,
    ) !PostResult {
        defer done.set(io);
        var req = try client.request(.POST, try std.Uri.parse(url), .{
            .redirect_behavior = .unhandled,
            .headers = .{
                .content_type = .{ .override = "application/json" },
                .user_agent = .{ .override = sdk_info },
                .authorization = .{ .override = authorization },
                // The body of the response is then not compressed.
                .accept_encoding = .omit,
            },
            .extra_headers = headers,
        });
        defer req.deinit();
        try req.sendBodyComplete(body);

        var res = try req.receiveHead(&.{});
        // Before the body is read: it can overwrite the head.
        var retry_after: ?u32 = null;
        var it = res.head.iterateHeaders();
        while (it.next()) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "retry-after")) retry_after = parseRetryAfter(h.value);
        }
        const status = res.head.status;
        var transfer_buffer: [64]u8 = undefined;
        _ = res.reader(&transfer_buffer).streamRemaining(&response.writer) catch |err| switch (err) {
            error.ReadFailed => return res.bodyErr().?,
            else => |e| return e,
        };
        return .{ .status = status, .retry_after = retry_after };
    }
};

/// A request body, and its `Content-Encoding` if it is compressed.
const Payload = struct {
    body: []u8,
    content_encoding: ?[]const u8 = null,
};

/// Compresses request bodies. The thread of the client makes one: the
/// compressor is large (approximately 224 KiB, and the window).
const Compressor = struct {
    gpa: std.mem.Allocator,
    container: std.compress.flate.Container,
    /// The state of `std.compress.flate.Compress`, which `compress` makes
    /// again for each body.
    state: std.compress.flate.Compress,
    window: [std.compress.flate.max_window_len]u8,
    output: std.Io.Writer.Allocating,

    fn create(gpa: std.mem.Allocator, container: std.compress.flate.Container) !*Compressor {
        const c = try gpa.create(Compressor);
        c.gpa = gpa;
        c.container = container;
        c.output = .init(gpa);
        return c;
    }

    fn destroy(c: *Compressor) void {
        c.output.deinit();
        c.gpa.destroy(c);
    }

    /// `input`, compressed. The slice is valid until the next `compress`.
    fn compress(c: *Compressor, input: []const u8) error{OutOfMemory}![]u8 {
        c.output.clearRetainingCapacity();
        // `Compress.init` asserts that the output has a buffer.
        try c.output.ensureUnusedCapacity(@max(input.len / 4, 64));
        // The only writer error is from `output`: out of memory.
        c.state = std.compress.flate.Compress.init(&c.output.writer, &c.window, c.container, .default) catch return error.OutOfMemory;
        c.state.writer.writeAll(input) catch return error.OutOfMemory;
        c.state.finish() catch return error.OutOfMemory;
        return c.output.written();
    }
};

/// The number of events at the start of `events` for one request.
fn batchLen(events: []const []u8) usize {
    var size: usize = 0;
    for (events, 0..) |event, i| {
        if (i == max_batch_events or (i > 0 and size + event.len > max_batch_size)) return i;
        size += event.len;
    }
    return events.len;
}

/// The `Retry-After` value in seconds. An HTTP date is ignored.
fn parseRetryAfter(value: []const u8) ?u32 {
    const seconds = std.fmt.parseUnsigned(u32, std.mem.trim(u8, value, " \t"), 10) catch return null;
    return @min(seconds, max_backoff_s);
}

/// A random UUID v4.
fn newUuid(io: std.Io) Uuid {
    var bytes: [16]u8 = undefined;
    io.random(&bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    var out: Uuid = undefined;
    _ = std.fmt.bufPrint(&out, "{x}-{x}-{x}-{x}-{x}", .{
        bytes[0..4], bytes[4..6], bytes[6..8], bytes[8..10], bytes[10..16],
    }) catch unreachable;
    return out;
}

/// The uuid of an event from `encodeEvent`, which writes it first.
fn eventUuid(json: []const u8) []const u8 {
    const prefix = "{\"uuid\":\"";
    return json[prefix.len..][0..@sizeOf(Uuid)];
}

const timestamp_len = "2026-10-20T12:34:56.789Z".len;

/// `ns` (Unix time) as RFC 3339 in UTC with milliseconds. The v1 server
/// rejects timestamps without a time zone.
fn formatTimestamp(buf: *[timestamp_len]u8, ns: i96) []const u8 {
    const ms: u64 = @intCast(@max(@divFloor(ns, std.time.ns_per_ms), 0));
    const epoch: std.time.epoch.EpochSeconds = .{ .secs = ms / std.time.ms_per_s };
    const year_day = epoch.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch.getDaySeconds();
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
        ms % std.time.ms_per_s,
    }) catch unreachable;
}

const EventHeader = struct {
    uuid: Uuid,
    event: []const u8,
    distinct_id: []const u8,
    timestamp_ns: i96,
    personless: bool,
};

/// The event as a v1 JSON event. The uuid is first, for `eventUuid`.
fn encodeEvent(gpa: std.mem.Allocator, header: EventHeader, properties: anytype) CaptureError![]u8 {
    var w: std.Io.Writer.Allocating = .init(gpa);
    defer w.deinit();
    writeEvent(&w.writer, header, properties) catch |err| switch (err) {
        error.WriteFailed => return error.OutOfMemory,
        else => |e| return e,
    };
    if (w.written().len > max_event_size) return error.EventTooLarge;
    return w.toOwnedSlice();
}

fn writeEvent(w: *std.Io.Writer, header: EventHeader, properties: anytype) !void {
    var timestamp_buf: [timestamp_len]u8 = undefined;
    try w.print("{{\"uuid\":\"{s}\",\"event\":", .{&header.uuid});
    try writeString(w, header.event);
    try w.writeAll(",\"distinct_id\":");
    try writeString(w, header.distinct_id);
    try w.print(",\"timestamp\":\"{s}\",\"options\":{{", .{formatTimestamp(&timestamp_buf, header.timestamp_ns)});
    if (header.personless) try w.writeAll("\"process_person_profile\":false");
    try w.writeAll("},\"properties\":");

    const T = @TypeOf(properties);
    // `.{}` is an empty tuple, which `std.json` writes as `[]`.
    const empty = T == @TypeOf(null) or (@typeInfo(T) == .@"struct" and @typeInfo(T).@"struct".field_names.len == 0);
    if (empty) {
        try w.writeAll("{}");
    } else {
        const start = w.end;
        try std.json.Stringify.value(properties, .{}, w);
        if (w.buffer[start] != '{') return error.PropertiesNotObject;
    }
    try w.writeByte('}');
}

/// The v1 batch envelope. The API key is in the `Authorization` header.
fn writeBatch(w: *std.Io.Writer, created_at: []const u8, events: []const []u8, historical_migration: bool) std.Io.Writer.Error!void {
    try w.print("{{\"created_at\":\"{s}\",", .{created_at});
    if (historical_migration) try w.writeAll("\"historical_migration\":true,");
    try w.writeAll("\"batch\":[");
    for (events, 0..) |event, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll(event);
    }
    try w.writeAll("]}");
}

/// `s` as a JSON string. JSON must be UTF-8: if `s` is not valid UTF-8,
/// each byte above 0x7f becomes U+FFFD.
fn writeString(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    if (std.unicode.utf8ValidateSlice(s)) return std.json.Stringify.encodeJsonString(s, .{}, w);
    try w.writeByte('"');
    for (s) |c| {
        if (c < 0x80) {
            try std.json.Stringify.encodeJsonStringChars(&.{c}, .{}, w);
        } else {
            try w.writeAll("\\ufffd");
        }
    }
    try w.writeByte('"');
}

const testing = std.testing;

test formatTimestamp {
    var buf: [timestamp_len]u8 = undefined;
    try testing.expectEqualStrings("1970-01-01T00:00:00.000Z", formatTimestamp(&buf, 0));
    // 2026-10-20T12:34:56.789Z
    try testing.expectEqualStrings("2026-10-20T12:34:56.789Z", formatTimestamp(&buf, 1792499696_789_000_000));
}

test newUuid {
    const uuid = newUuid(testing.io);
    try testing.expectEqual('-', uuid[8]);
    try testing.expectEqual('-', uuid[13]);
    try testing.expectEqual('4', uuid[14]);
    try testing.expect(std.mem.indexOfScalar(u8, "89ab", uuid[19]) != null);
}

test encodeEvent {
    const header: EventHeader = .{
        .uuid = "0b7f2d4e-1c3a-4f5b-9d6e-7a8b9c0d1e2f".*,
        .event = "signed \"up\"",
        .distinct_id = "user\xff",
        .timestamp_ns = 0,
        .personless = false,
    };
    const json = try encodeEvent(testing.allocator, header, .{ .plan = "free", .seats = 3 });
    defer testing.allocator.free(json);
    try testing.expectEqualStrings(
        \\{"uuid":"0b7f2d4e-1c3a-4f5b-9d6e-7a8b9c0d1e2f","event":"signed \"up\"","distinct_id":"user\ufffd","timestamp":"1970-01-01T00:00:00.000Z","options":{},"properties":{"plan":"free","seats":3}}
    , json);
    try testing.expectEqualStrings("0b7f2d4e-1c3a-4f5b-9d6e-7a8b9c0d1e2f", eventUuid(json));

    var personless = header;
    personless.personless = true;
    const empty = try encodeEvent(testing.allocator, personless, .{});
    defer testing.allocator.free(empty);
    try testing.expect(std.mem.endsWith(u8, empty, "\"options\":{\"process_person_profile\":false},\"properties\":{}}"));

    try testing.expectError(error.PropertiesNotObject, encodeEvent(testing.allocator, header, @as(u32, 3)));
}

test "set, alias and groupIdentify properties" {
    const client = try Client.init(testing.allocator, testing.io, .{ .api_key = "phc_test", .host = "https://eu.i.posthog.com/" });
    defer client.destroy();
    try testing.expectEqualStrings("https://eu.i.posthog.com/i/v1/analytics/events", client.url);
    try testing.expectEqualStrings("Bearer phc_test", client.authorization);

    _ = try client.set("user1", .{ .name = "Max" });
    _ = try client.alias("anon1", "user1");
    _ = try client.groupIdentify("company", "acme", "user1", .{ .seats = 5 });
    const uuid = try client.capture("anon_event", null, null);
    try testing.expectError(error.MissingId, client.alias("", "user1"));

    const q = client.queue.items;
    try testing.expectEqual(4, q.len);
    try testing.expect(std.mem.indexOf(u8, q[0], "\"event\":\"$set\",\"distinct_id\":\"user1\"") != null);
    try testing.expect(std.mem.endsWith(u8, q[0], "\"properties\":{\"$set\":{\"name\":\"Max\"}}}"));
    try testing.expect(std.mem.indexOf(u8, q[1], "\"distinct_id\":\"anon1\"") != null);
    try testing.expect(std.mem.endsWith(u8, q[1], "\"properties\":{\"distinct_id\":\"anon1\",\"alias\":\"user1\"}}"));
    try testing.expect(std.mem.endsWith(u8, q[2], "\"properties\":{\"$group_type\":\"company\",\"$group_key\":\"acme\",\"$group_set\":{\"seats\":5}}}"));
    try testing.expectEqualStrings(&uuid, eventUuid(q[3]));
    try testing.expect(std.mem.indexOf(u8, q[3], "\"process_person_profile\":false") != null);
}

test writeBatch {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    var a = "{\"a\":1}".*;
    var b = "{\"b\":2}".*;
    var events = [_][]u8{ &a, &b };
    try writeBatch(&w, "1970-01-01T00:00:00.000Z", &events, true);
    try testing.expectEqualStrings(
        \\{"created_at":"1970-01-01T00:00:00.000Z","historical_migration":true,"batch":[{"a":1},{"b":2}]}
    , w.buffered());
}

test "retainRetries" {
    const client = try Client.init(testing.allocator, testing.io, .{ .api_key = "phc_test" });
    defer client.destroy();
    var header: EventHeader = .{ .uuid = undefined, .event = "e", .distinct_id = "d", .timestamp_ns = 0, .personless = false };
    var events: [4][]u8 = undefined;
    for (&events, "0123") |*event, c| {
        header.uuid = ("0000000" ++ [_]u8{c} ++ "-0000-4000-8000-000000000000").*;
        event.* = try encodeEvent(testing.allocator, header, null);
    }
    var dropped: usize = 0;
    const n = try client.retainRetries(&events,
        \\{"results":{"00000000-0000-4000-8000-000000000000":{"result":"ok"},
        \\"00000001-0000-4000-8000-000000000000":{"result":"retry"},
        \\"00000002-0000-4000-8000-000000000000":{"result":"drop","details":"invalid"}},"extra":1}
    , &dropped);
    defer for (events[0..n]) |event| testing.allocator.free(event);
    // Event 3 is not in the results: it is accepted.
    try testing.expectEqual(1, n);
    try testing.expectEqual(1, dropped);
    try testing.expectEqualStrings("00000001-0000-4000-8000-000000000000", eventUuid(events[0]));
}

test Compressor {
    const flate = std.compress.flate;
    var input: [100_000]u8 = undefined;
    for (&input, 0..) |*b, i| b.* = "{\"event\":\"zig_bulk_event\",\"index\":"[i % 34];

    for ([_]Compression{ .gzip, .deflate }) |compression| {
        const c = try Compressor.create(testing.allocator, compression.container().?);
        defer c.destroy();
        // Two bodies, as the thread of the client compresses them.
        for (0..2) |_| {
            const compressed = try c.compress(&input);
            try testing.expect(compressed.len < input.len / 10);
            if (compression == .gzip) try testing.expectEqualSlices(u8, &.{ 0x1f, 0x8b }, compressed[0..2]);
            if (compression == .deflate) try testing.expectEqual(0x78, compressed[0]);

            var reader: std.Io.Reader = .fixed(compressed);
            var window: [flate.max_window_len]u8 = undefined;
            var decompress: flate.Decompress = .init(&reader, compression.container().?, &window);
            const output = try decompress.reader.allocRemaining(testing.allocator, .unlimited);
            defer testing.allocator.free(output);
            try testing.expectEqualSlices(u8, &input, output);
        }
    }
}

test batchLen {
    var small = "{}".*;
    var events: [max_batch_events + 5][]u8 = @splat(&small);
    try testing.expectEqual(max_batch_events, batchLen(&events));
    try testing.expectEqual(5, batchLen(events[max_batch_events..]));
}

test parseRetryAfter {
    try testing.expectEqual(5, parseRetryAfter(" 5"));
    try testing.expectEqual(max_backoff_s, parseRetryAfter("3600"));
    try testing.expectEqual(null, parseRetryAfter("Wed, 21 Oct 2026 07:28:00 GMT"));
}
