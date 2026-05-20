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
chrononlabs_streamnet_stats
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



# ChrononLabs-Stream-Net Bonus examples (skip if you don't care, mostly for people that plans using it into big project / where fine-control is required)

These are extra examples for ChrononLabsStreamNet, they cover normal usage, large sanitized table streaming, raw transfers, completion callbacks, ACK/NACK behavior, retry settings, timeout settings, and the full options table.

Important:
ChrononLabsStreamNet can serialize normal Lua values, tables, numbers, strings, booleans, vectors, angles, colors, and entities. (It cannot serialize functions!!)

So if you want to send something like `hook.GetTable ()` or `concommand.GetTable ()`, you must sanitize it first, because those tables contain functions.

## Example 1: Reliable settings sync with completion callback

This example sends a normal structured table from the server to one client.

The library handles chunking, compression, ACK/NACK, retry, timeout, and final delivery automatically.

### Server

```lua
-- util.AddNetworkString ("ForcePlayerSettingsSync") -- Not needed for ChrononLabsStreamNet, just an example of what you no longer need! It is handled automatically

hook.Add ("PlayerInitialSpawn", "ChrononLabsStreamNetExampleSettings", function (ply)
    timer.Simple (3, function ()
        if not IsValid (ply) then return end

        local settings = {
            Version = 1,
            ServerName = GetHostName (),
            Features = {
                Inventory = true,
                Anticheat = true,
                BuildMode = false
            },
            Limits = {
                MaxProps = 500,
                MaxVehicles = 10,
                MaxUploads = 3
            }
        }

        ChrononLabsStreamNet.SendEx ("ExampleSettingsSync", ply, {
            ChunkSize = 16384,
            Compress = true,
            CompressAt = 1024,
            RetryInterval = 0.75,
            Timeout = 20,
            MaximumRetries = 16,
            Window = 6,
            ReliableData = false,

            OnComplete = function (ok, reason, transfer)
                print ("[SettingsSync] complete:", ok, reason)
                print ("[SettingsSync] transfer id:", transfer.Id)
                print ("[SettingsSync] chunks acked:", transfer.AckCount .. "/" .. transfer.TotalChunks)
                print ("[SettingsSync] compressed:", transfer.Compressed)
            end
        }, settings)
    end)
end)
```

### Client

```lua
ChrononLabsStreamNet.Receive ("ExampleSettingsSync", function (settings)
    print ("Received server settings")
    PrintTable (settings)
end)
```

### What this example covers

- `SendEx`
- Structured table sending
- Compression
- Automatic ACK tracking
- Automatic retry if chunks are missing
- Timeout failure if the transfer cannot complete
- `OnComplete` callback after the receiver confirms completion


## Example 2: Send large sanitized `concommand.GetTable ()` and `hook.GetTable ()`

This example sends big Garry's Mod tables without crashing on function values.

It converts unsupported values into safe strings before sending.

This is useful for debug tools, admin tools, anticheat telemetry, addon inspection, or developer panels.

### Shared sanitizer

Put this somewhere shared, or above your example code.

```lua
local function ChrononLabsStreamNetSafeCopy (value, depth, seen)
    depth = depth or 0
    seen = seen or {}

    local valueType = type (value)

    if valueType == "nil" then return nil end
    if valueType == "boolean" then return value end
    if valueType == "number" then return value end
    if valueType == "string" then return value end

    if IsEntity and IsEntity (value) then
        if IsValid (value) then
            return tostring (value)
        end

        return "NULL Entity"
    end

    if isvector and isvector (value) then
        return {
            Type = "Vector",
            X = value.x,
            Y = value.y,
            Z = value.z
        }
    end

    if isangle and isangle (value) then
        return {
            Type = "Angle",
            P = value.p,
            Y = value.y,
            R = value.r
        }
    end

    if IsColor and IsColor (value) then
        return {
            Type = "Color",
            R = value.r,
            G = value.g,
            B = value.b,
            A = value.a
        }
    end

    if valueType == "function" then
        return "[function]"
    end

    if valueType == "userdata" then
        return "[userdata] " .. tostring (value)
    end

    if valueType == "thread" then
        return "[thread] " .. tostring (value)
    end

    if valueType ~= "table" then
        return "[" .. valueType .. "] " .. tostring (value)
    end

    if depth >= 8 then
        return "[max depth]"
    end

    if seen [value] then
        return "[cycle]"
    end

    seen [value] = true

    local output = {}
    local count = 0
    local maximumEntries = 2500

    for key, pairValue in pairs (value) do
        count = count + 1

        if count > maximumEntries then
            output ["__truncated"] = true
            output ["__truncatedReason"] = "Maximum entries reached at this table level."
            break
        end

        local safeKey = ChrononLabsStreamNetSafeCopy (key, depth + 1, seen)

        if type (safeKey) ~= "string" and type (safeKey) ~= "number" then
            safeKey = tostring (safeKey)
        end

        output [safeKey] = ChrononLabsStreamNetSafeCopy (pairValue, depth + 1, seen)
    end

    seen [value] = nil

    return output
end
```

### Client requests the dump

```lua
concommand.Add ("clsnet_request_debug_dump", function ()
    ChrononLabsStreamNet.Send ("ExampleRequestDebugDump", {
        IncludeConCommands = true,
        IncludeHooks = true
    })
end)
```

### Server builds and sends the sanitized dump

```lua
ChrononLabsStreamNet.Receive ("ExampleRequestDebugDump", function (ply, request)
    if not IsValid (ply) then return end

    -- Always permission check real debug/admin features.
    if not ply:IsAdmin () then return end

    local dump = {
        GeneratedAt = os.time (),
        RequestedBy = ply:SteamID64 (),
        ServerName = GetHostName (),
        ConCommands = request.IncludeConCommands and ChrononLabsStreamNetSafeCopy (concommand.GetTable ()) or nil,
        Hooks = request.IncludeHooks and ChrononLabsStreamNetSafeCopy (hook.GetTable ()) or nil
    }

    local json = util.TableToJSON (dump, false)

    ChrononLabsStreamNet.SendRaw ("ExampleDebugDump", ply, json, {
        ChunkSize = 16384,
        Compress = true,
        CompressAt = 512,
        RetryInterval = 0.75,
        Timeout = 30,
        MaximumRetries = 20,
        Window = 8,
        ReliableData = false,

        OnComplete = function (ok, reason, transfer)
            print ("[DebugDump] sent:", ok, reason)
            print ("[DebugDump] raw size:", transfer.RawSize)
            print ("[DebugDump] packed size:", transfer.PackedSize)
            print ("[DebugDump] chunks:", transfer.TotalChunks)
            print ("[DebugDump] acked:", transfer.AckCount)
            print ("[DebugDump] retransmits may have happened if chunk retries were needed")
        end
    })
end)
```

### Client receives the raw JSON dump

```lua
ChrononLabsStreamNet.Receive ("ExampleDebugDump", function (json)
    local dump = util.JSONToTable (json)

    if not dump then
        print ("Failed to decode debug dump")
        return
    end

    print ("Received debug dump")
    print ("Generated at:", dump.GeneratedAt)
    print ("ConCommand entries:", dump.ConCommands and table.Count (dump.ConCommands) or 0)
    print ("Hook entries:", dump.Hooks and table.Count (dump.Hooks) or 0)

    -- PrintTable (dump) can be huge.
    -- Use it only if you really want the full output.
end)
```

### What this example covers

- Sending very large data
- Sanitizing unsupported values
- Avoiding function serialization errors
- Raw binary/string transfer with `SendRaw`
- Compression
- ACK/NACK recovery
- Retry tracking through `OnComplete`
- Admin permission check pattern


## Bonus example: All options and useful transfer fields

This example shows the full per-transfer options table and the useful fields available inside `OnComplete`.

### Per-transfer options

These can be passed to `SendEx` or `SendRaw`.

```lua
local options = {
    -- Maximum data bytes per chunk.
    -- Keep this safely below the net message limit.
    ChunkSize = 16384,

    -- Enables or disables util.Compress for this transfer.
    Compress = true,

    -- Only compress when the original payload is at least this many bytes.
    CompressAt = 1024,

    -- How long to wait before retrying a chunk that was not acknowledged.
    RetryInterval = 0.75,

    -- How long the sender waits without progress before failing the transfer.
    Timeout = 25,

    -- Maximum retry count per chunk.
    MaximumRetries = 20,

    -- Maximum number of unacknowledged chunks in flight.
    Window = 6,

    -- false means data chunks are sent using unreliable mode and repaired by ACK/NACK.
    -- true means data chunks are sent reliable, which is not recommended for very large transfers.
    ReliableData = false,

    -- Called when the receiver confirms completion, or when the transfer fails.
    OnComplete = function (ok, reason, transfer)
        print ("Transfer finished:", ok, reason)

        -- Useful transfer fields:
        print ("Id:", transfer.Id)
        print ("Name:", transfer.Name)
        print ("Mode:", transfer.Mode)
        print ("RawSize:", transfer.RawSize)
        print ("PackedSize:", transfer.PackedSize)
        print ("Compressed:", transfer.Compressed)
        print ("Checksum:", transfer.Checksum)
        print ("ChunkSize:", transfer.ChunkSize)
        print ("TotalChunks:", transfer.TotalChunks)
        print ("AckCount:", transfer.AckCount)
        print ("InFlightCount:", transfer.InFlightCount)
        print ("RetryInterval:", transfer.RetryInterval)
        print ("Timeout:", transfer.Timeout)
        print ("MaximumRetries:", transfer.MaximumRetries)
        print ("Window:", transfer.Window)
        print ("ReliableData:", transfer.ReliableData)
        print ("CreatedAt:", transfer.CreatedAt)
        print ("LastProgress:", transfer.LastProgress)
        print ("Done:", transfer.Done)

        -- Detailed internal tables:
        -- transfer.Sent
        -- transfer.Retries
        -- transfer.Acked
        -- transfer.InFlight
        -- transfer.NackQueue
        -- transfer.NackSeen

        -- Avoid printing transfer.Data for huge payloads.
    end
}
```

### Global configuration fields

These are global defaults. Set them once before heavy use.

```lua
ChrononLabsStreamNet.SetConfig ("MaximumNetMessageBytes", 60000)
ChrononLabsStreamNet.SetConfig ("ChunkSize", 16384)
ChrononLabsStreamNet.SetConfig ("BytesPerSecond", 98304)
ChrononLabsStreamNet.SetConfig ("BurstBytes", 65536)
ChrononLabsStreamNet.SetConfig ("Window", 6)
ChrononLabsStreamNet.SetConfig ("RetryInterval", 0.75)
ChrononLabsStreamNet.SetConfig ("Timeout", 20)
ChrononLabsStreamNet.SetConfig ("MaximumRetries", 16)
ChrononLabsStreamNet.SetConfig ("Compress", true)
ChrononLabsStreamNet.SetConfig ("CompressAt", 1024)
ChrononLabsStreamNet.SetConfig ("MaximumPayloadBytes", 8 * 1024 * 1024)
ChrononLabsStreamNet.SetConfig ("MaximumIncomingTransfersPerPeer", 24)
ChrononLabsStreamNet.SetConfig ("MaximumTablePairs", 4096)
ChrononLabsStreamNet.SetConfig ("MaximumTableDepth", 32)
ChrononLabsStreamNet.SetConfig ("AckInterval", 0.035)
ChrononLabsStreamNet.SetConfig ("NackInterval", 0.35)
ChrononLabsStreamNet.SetConfig ("AckBatch", 64)
ChrononLabsStreamNet.SetConfig ("NackBatch", 64)
ChrononLabsStreamNet.SetConfig ("MaximumPacketsPerThink", 24)
ChrononLabsStreamNet.SetConfig ("QueueUntilClientReady", false)
ChrononLabsStreamNet.SetConfig ("Debug", false)
```

### Server sends a large generated payload with every option

```lua
concommand.Add ("clsnet_bonus_send_big", function (ply)
    if SERVER and IsValid (ply) then return end

    for _, target in ipairs (player.GetAll ()) do
        local payload = string.rep ("ChrononLabsStreamNet bonus payload\n", 50000)

        ChrononLabsStreamNet.SendRaw ("ExampleBonusBigPayload", target, payload, {
            ChunkSize = 16384,
            Compress = true,
            CompressAt = 1024,
            RetryInterval = 0.75,
            Timeout = 25,
            MaximumRetries = 20,
            Window = 6,
            ReliableData = false,

            OnComplete = function (ok, reason, transfer)
                print ("[Bonus] complete:", ok, reason)
                print ("[Bonus] target:", IsValid (transfer.Peer) and transfer.Peer:Nick () or "invalid")
                print ("[Bonus] transfer:", transfer.Id, transfer.Name)
                print ("[Bonus] size:", transfer.RawSize, "packed:", transfer.PackedSize)
                print ("[Bonus] chunks:", transfer.AckCount .. "/" .. transfer.TotalChunks)
                print ("[Bonus] compressed:", transfer.Compressed)
            end
        })
    end
end)
```

### Client receives the large payload

```lua
ChrononLabsStreamNet.Receive ("ExampleBonusBigPayload", function (payload)
    print ("Received bonus payload")
    print ("Payload size:", #payload)
    print ("First 128 bytes:")
    print (string.sub (payload, 1, 128))
end)
```

### What this bonus example covers

- Every per-transfer option
- Global configuration fields
- Raw transfer
- Large generated payload
- Completion callback
- ACK count
- Total chunk count
- Retry and timeout configuration
- ReliableData choice
- Compression choice
- Transfer metadata inspection


## Notes++ about ACK, NACK, retry, and timeout

!You do not manually call ACK or NACK!

The flow is automatic:

1. Sender splits the payload into chunks.
2. Sender sends chunks according to pacing and window limits.
3. Receiver ACKs chunks that arrived correctly.
4. Receiver NACKs chunks that are missing or corrupted.
5. Sender retries missing chunks.
6. Receiver assembles the full payload only when every chunk is present.
7. Receiver validates the final payload checksum.
8. Receiver calls your `Receive` callback.
9. Receiver sends final completion confirmation.
10. Sender calls `OnComplete`.

If this cannot complete before the timeout or retry limit, the transfer fails properly in an 'expected way' that you can handle.
Contributions are welcomed of course;

