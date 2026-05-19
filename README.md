# ChrononLabsStreamNet

ChrononLabsStreamNet is a single-file streaming networking library for Garry's Mod.

It is designed to make networking cleaner, safer, and more reliable across all kinds of projects. You can use it for simple addon messages, structured data sync, raw binary transfers, or very large payloads that would normally be painful to handle with the default `net` library.

It is not only useful for large payloads. It can also be used as a better organized and more optimized networking layer for normal project communication, while still being powerful enough for complex systems that need guaranteed complete delivery.

## Why it exists

Garry's Mod networking has strict size limits and can become unstable if large data is sent too aggressively. ChrononLabsStreamNet handles this by automatically splitting, pacing, validating, retrying, and rebuilding transfers.

Instead of manually writing your own chunking system every time, you get one unified API that can handle both small and large messages.

## Main advantages

- Single-file library
- Simple unified API
- Works client to server and server to client
- Suitable for both small and large messages
- More structured than manually managing `net.Start`, `net.Write*`, and `net.Receive`
- Automatic chunking for large payloads
- Per-peer pacing to reduce net buffer pressure
- Optional compression using `util.Compress`
- ACK and NACK recovery system
- Automatic retry of missing chunks
- Transfer timeout handling
- Full payload validation before delivery
- Checksum validation for chunks and complete transfers
- Completion callbacks
- Raw binary streaming mode
- Useful for basic addons, advanced systems, admin tools, anticheat systems, AI telemetry, save systems, and file-like transfers

## Delivery model

ChrononLabsStreamNet provides guaranteed complete delivery semantics.

A message is only delivered to your callback once the full payload has been received, validated, assembled, and decompressed if needed.

If a chunk is missing, it can be requested again. If the transfer cannot recover, it fails instead of silently delivering incomplete or corrupted data.

This makes it useful for systems where partial delivery is not acceptable.

## Installation

Place the file somewhere shared, for example:

```lua
lua/autorun/chrononlabs-stream-net.lua
```

The file handles client distribution automatically when loaded server-side.

## Basic usage

### Server

```lua
ChrononLabsStreamNet.Receive ("ClientHello", function (ply, message)
    print ("Client said:", ply, message)

    ChrononLabsStreamNet.Send ("ServerReply", ply, "Hello from the server")
end)
```

### Client

```lua
ChrononLabsStreamNet.Receive ("ServerReply", function (message)
    print ("Server replied:", message)
end)

ChrononLabsStreamNet.Send ("ClientHello", "Hello from the client")
```

## Sending tables

### Server

```lua
ChrononLabsStreamNet.Send ("PlayerData", ply, {
    Name = "Player",
    Level = 25,
    Position = Vector (100, 200, 300),
    Stats = {
        Health = 100,
        Armor = 50
    }
})
```

### Client

```lua
ChrononLabsStreamNet.Receive ("PlayerData", function (data)
    PrintTable (data)
end)
```

## Broadcasting from the server

```lua
ChrononLabsStreamNet.Broadcast ("GlobalAnnouncement", {
    Title = "Server Update",
    Message = "A new system has been loaded."
})
```

## Sending raw binary data

Raw mode is useful when you already have your own encoded format, compressed blob, generated file content, or binary payload.

### Client

```lua
local data = string.rep ("A", 300000)

ChrononLabsStreamNet.SendRaw ("UploadBlob", data, {
    ChunkSize = 16384,
    BytesPerSecond = 128 * 1024,
    Window = 8,
    OnComplete = function (ok, reason)
        print ("Upload complete:", ok, reason)
    end
})
```

### Server

```lua
ChrononLabsStreamNet.Receive ("UploadBlob", function (ply, bytes)
    print ("Received upload from", ply, "size:", #bytes)
end)
```

## Custom transfer options

```lua
ChrononLabsStreamNet.SendEx ("LargeInventorySync", ply, {
    ChunkSize = 16384,
    BytesPerSecond = 96 * 1024,
    BurstBytes = 64 * 1024,
    Window = 6,
    RetryInterval = 0.75,
    Timeout = 20,
    MaximumRetries = 16,
    Compress = true,
    OnComplete = function (ok, reason, transfer)
        print ("Transfer result:", ok, reason)
    end
}, inventoryData)
```

## Global configuration

You can adjust global settings before heavy use:

```lua
ChrononLabsStreamNet.SetConfig ("ChunkSize", 16384)
ChrononLabsStreamNet.SetConfig ("BytesPerSecond", 98304)
ChrononLabsStreamNet.SetConfig ("BurstBytes", 65536)
ChrononLabsStreamNet.SetConfig ("Window", 6)
ChrononLabsStreamNet.SetConfig ("Timeout", 20)
ChrononLabsStreamNet.SetConfig ("MaximumRetries", 16)
```

## Stats

The library includes a simple console command:

```txt
chrononlabsstreamnetstats
```

You can also access stats manually:

```lua
PrintTable (ChrononLabsStreamNet.GetStats ())
```

## Example use cases

ChrononLabsStreamNet can be used for:

- Addon configuration sync
- Inventory systems
- Character data
- Duplication data
- Build data
- Save and load systems
- Large anticheat reports
- AI or machine learning telemetry
- Custom file transfer
- Admin tools
- Complex UI state syncing
- Server to client cache replication
- Large generated datasets
- Any system where normal net messages become too limited or messy

## Best practices

Do not trust client data just because the transport succeeded!!

ChrononLabsStreamNet improves delivery, pacing, recovery, and structure. It does not replace server-side validation, permission checks, sanity checks, or anticheat logic.

For client to server messages, always validate that the player is allowed to send the data they are sending as well as the payload sent!

## Notes

ChrononLabsStreamNet is meant to be a practical general-purpose networking layer.

For tiny one-off messages, the default Garry's Mod `net` library is still fine. ChrononLabsStreamNet becomes useful when you want cleaner structure, better reliability, larger transfers, recovery logic, or a unified API across your project.

## License

This project is licensed under the MIT License.

You may use, modify, distribute, and include it in your own projects, including commercial addons, as long as the original copyright and license notice are preserved.

If the file is renamed, merged, or implemented directly inside another addon, please keep clear credit to ChrononLabsStreamNet / ChrononLabs in the source or documentation.
