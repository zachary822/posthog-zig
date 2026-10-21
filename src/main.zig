//! Sends one event to PostHog:
//!
//!     POSTHOG_API_KEY=phc_... zig build run -- <event> [distinct_id]
//!
//! `POSTHOG_HOST` changes the host (the default is the US host).
//! `POSTHOG_COMPRESSION` is `none` (the default), `gzip` or `deflate`.

const std = @import("std");
const posthog = @import("posthog");

pub fn main(init: std.process.Init) !u8 {
    const api_key = init.environ_map.get("POSTHOG_API_KEY") orelse {
        std.log.err("POSTHOG_API_KEY is not set", .{});
        return 1;
    };
    const host = init.environ_map.get("POSTHOG_HOST") orelse posthog.us_host;
    const compression_name = init.environ_map.get("POSTHOG_COMPRESSION") orelse "none";
    const compression = std.meta.stringToEnum(posthog.Compression, compression_name) orelse {
        std.log.err("POSTHOG_COMPRESSION must be none, gzip or deflate, not \"{s}\"", .{compression_name});
        return 1;
    };

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2 or args.len > 3) {
        std.log.err("usage: {s} <event> [distinct_id]", .{args[0]});
        return 1;
    }
    const distinct_id: ?[]const u8 = if (args.len == 3) args[2] else null;

    const client = try posthog.Client.create(init.gpa, init.io, .{
        .api_key = api_key,
        .host = host,
        .compression = compression,
    });
    // Sends the event before the program stops.
    defer client.destroy();

    const uuid = try client.capture(args[1], distinct_id, .{
        .source = "posthog-zig example",
        .zig_version = @import("builtin").zig_version_string,
    });
    std.log.info("captured {s} ({s})", .{ args[1], &uuid });
    return 0;
}
