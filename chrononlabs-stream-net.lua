--[[
	ChrononLabs-Stream-Net
	Single file streamed networking layer for Garry's Mod.

	Written by Enumerator (@makeconstructor) - 20/05/2026

	Main usage:
		ChrononLabsStreamNet.Receive (name, callback)
		ChrononLabsStreamNet.Send    (name, [target], ...)   -- target is server-side only
		ChrononLabsStreamNet.Broadcast(name, ...)            -- server only
		ChrononLabsStreamNet.Request (name, [target], data, options, callback)
		ChrononLabsStreamNet.Respond (name, policy, callback)

	For the full API (SendEx, SendRaw, options, stats, etc...), see the README:
		https://github.com/InfHorus/Chrononlabs-StreamNet
]]

if SERVER then
	AddCSLuaFile ()
end

ChrononLabsStreamNet  = ChrononLabsStreamNet or {}

local library         	= ChrononLabsStreamNet
local channelName     	= "ChrononLabsStreamNet"
local protocolVersion 	= 1
local minimumChunkSize 	= 512

local packetData     = 1
local packetAck      = 2
local packetNack     = 3
local packetComplete = 4
local packetCancel   = 5
local packetReady    = 6

local modeArguments = 1
local modeRaw       = 2

local internalNamePrefix = "__chrononlabs_streamnet:"
local requestNamePrefix  = internalNamePrefix .. "request:"
local responseName       = internalNamePrefix .. "response"

local tagNil    = 0
local tagFalse  = 1
local tagTrue   = 2
local tagInt32  = 3
local tagNumber = 4
local tagString = 5
local tagTable  = 6
local tagVector = 7
local tagAngle  = 8
local tagColor  = 9
local tagEntity = 10

local mathAbs      = math.abs
local mathCeil     = math.ceil
local mathFloor    = math.floor
local mathFrexp    = math.frexp
local mathHuge     = math.huge
local mathLdexp    = math.ldexp
local mathMax      = math.max
local mathMin      = math.min
local stringByte   = string.byte
local stringChar   = string.char
local stringLower  = string.lower
local stringSub    = string.sub
local tableConcat  = table.concat
local tableSort    = table.sort
local tableUnpack  = unpack
local type         = type
local tostring     = tostring
local tonumber     = tonumber
local pairs        = pairs
local ipairs       = ipairs
local pcall        = pcall
local next         = next
local select       = select
local assert       = assert
local error        = error

local IsValid          = IsValid
local IsEntity         = IsEntity
local isvector         = isvector
local isangle          = isangle
local IsColor          = IsColor
local Vector           = Vector
local Angle            = Angle
local Color            = Color
local Entity           = Entity
local NULL             = NULL
local RealTime         = RealTime
local playerIterator   = player and player.Iterator
local hookAdd          = hook.Add
local utilCRC          = util.CRC
local utilCompress     = util.Compress
local utilDecompress   = util.Decompress
local netStart         = net.Start
local netSend          = net.Send
local netSendToServer  = net.SendToServer
local netReceive       = net.Receive
local netWriteUInt     = net.WriteUInt
local netReadUInt      = net.ReadUInt
local netWriteBool     = net.WriteBool
local netReadBool      = net.ReadBool
local netWriteString   = net.WriteString
local netReadString    = net.ReadString
local netWriteData     = net.WriteData
local netReadData      = net.ReadData

library.Config = library.Config or {}
local config   = library.Config

config.ChannelName                     = config.ChannelName or channelName
config.MaximumNetMessageBytes          = config.MaximumNetMessageBytes or 60000
config.ChunkSize                       = config.ChunkSize or 16384
config.BytesPerSecond                  = config.BytesPerSecond or 98304
config.BurstBytes                      = config.BurstBytes or 65536
config.Window                          = config.Window or 6
config.RetryInterval                   = config.RetryInterval or 0.75
config.Timeout                         = config.Timeout or 20
config.MaximumRetries                  = config.MaximumRetries or 16
config.Compress                        = config.Compress ~= false
config.CompressAt                      = config.CompressAt or 8192
config.MaximumPayloadBytes             = config.MaximumPayloadBytes or 8 * 1024 * 1024
config.MaximumIncomingTransfersPerPeer = config.MaximumIncomingTransfersPerPeer or 24
config.MaximumTablePairs               = config.MaximumTablePairs or 4096
config.MaximumTableDepth               = config.MaximumTableDepth or 32
config.AckInterval                     = config.AckInterval or 0.035
config.NackInterval                    = config.NackInterval or 0.35
config.AckBatch                        = config.AckBatch or 64
config.NackBatch                       = config.NackBatch or 64
config.MaximumPacketsPerThink          = config.MaximumPacketsPerThink or 24
config.FinishedIncomingTtl             = config.FinishedIncomingTtl or mathMax (config.Timeout, 30)
config.MaximumFinishedIncomingPerPeer  = config.MaximumFinishedIncomingPerPeer or 256
config.FinishedControlResendInterval   = config.FinishedControlResendInterval or 0.25
config.PriorityAgingInterval           = config.PriorityAgingInterval or 2
config.QueueUntilClientReady           = config.QueueUntilClientReady or false
config.RequestTimeout                  = config.RequestTimeout or 15
config.Debug                           = config.Debug or false

channelName = config.ChannelName

library.Handlers           = library.Handlers or {}
library.ReceivePolicies    = library.ReceivePolicies or {}
library.ReceivePolicyState = library.ReceivePolicyState or {}
library.OutgoingStates     = library.OutgoingStates or {}
library.IncomingStates     = library.IncomingStates or {}
library.FinishedIncoming   = library.FinishedIncoming or {}
library.AckPending         = library.AckPending or {}
library.NackPending        = library.NackPending or {}
library.ReadyPlayers       = library.ReadyPlayers or {}
library.PlayersByUserId    = library.PlayersByUserId or {}
library.Profiles           = library.Profiles or {}
library.NextTransferId     = library.NextTransferId or math.random (1, 2147483000)
library.NextRequestId      = library.NextRequestId or math.random (1, 2147483000)
library.PendingRequests    = library.PendingRequests or {}
library.Metrics            = library.Metrics or {
	SentBytes      = 0,
	ReceivedBytes  = 0,
	SentChunks     = 0,
	ReceivedChunks = 0,
	Retransmits    = 0,
	Completed      = 0,
	Failed         = 0
}

if SERVER then
	util.AddNetworkString (channelName)
end

local function now ()
	if RealTime then return RealTime () end
	return os.clock ()
end

local function clamp (numberValue, lowerValue, upperValue)
	if numberValue < lowerValue then return lowerValue end
	if numberValue > upperValue then return upperValue end
	return numberValue
end

local function debugPrint (...)
	if not config.Debug then return end
	print ("(ChrononLabs-StreamNet)", ...)
end

local function lowerName (name)
	return stringLower (tostring (name or ""))
end

local function isReservedMessageName (name)
	return stringSub (lowerName (name), 1, #internalNamePrefix) == internalNamePrefix
end

local function validatePublicMessageName (name)
	if isReservedMessageName (name) then
		return false, "(ChrononLabs-StreamNet): Reserved message name. Names starting with '" .. internalNamePrefix .. "' are used internally."
	end

	return true
end

local function isPlayerValue (value)
	return TypeID (value) == TYPE_ENTITY and IsValid (value) and value:IsPlayer ()
end

local function nextTransferId ()
	library.NextTransferId = library.NextTransferId + 1
	if library.NextTransferId >= 4294967295 then
		library.NextTransferId = 1
	end

	return library.NextTransferId
end

local function nextRequestId ()
	library.NextRequestId = library.NextRequestId + 1
	if library.NextRequestId >= 4294967295 then
		library.NextRequestId = 1
	end

	return library.NextRequestId
end

local function crc (data)
	return tostring (utilCRC (data or ""))
end

-- Before getting any more complaints: I did it this way because from my profiler testings
-- sending one 32 caused a small client frame hitch that doesn't occur with two 16 despite the small theoretical overhead.
local function writeNetUnsigned32 (numberValue)
	numberValue = mathFloor (numberValue or 0) % 4294967296

	local lowWord  = numberValue % 65536
	local highWord = mathFloor (numberValue / 65536) % 65536

	netWriteUInt (lowWord, 16)
	netWriteUInt (highWord, 16)
end

local function readNetUnsigned32 ()
	local lowWord  = netReadUInt (16)
	local highWord = netReadUInt (16)

	return lowWord + highWord * 65536
end

local function startPacket (packetKind, unreliable)
	netStart (channelName, unreliable == true)
	netWriteUInt (packetKind, 4)
	netWriteUInt (protocolVersion, 4)
end

local function sendCurrentPacket (peer)
	if SERVER then
		if not isPlayerValue (peer) then return false end
		netSend (peer)
	else
		netSendToServer ()
	end

	return true
end

local function writeUnsigned8 (output, numberValue)
	output [#output + 1] = stringChar (numberValue % 256)
end

local function writeUnsigned16 (output, numberValue)
	numberValue          = mathFloor (numberValue or 0)
	output [#output + 1] = stringChar (
		numberValue % 256,
		mathFloor (numberValue / 256) % 256
	)
end

local function writeUnsigned32 (output, numberValue)
	numberValue = mathFloor (numberValue or 0)

	if numberValue < 0 then
		numberValue = numberValue + 4294967296
	end

	output [#output + 1] = stringChar (
		numberValue % 256,
		mathFloor (numberValue / 256) % 256,
		mathFloor (numberValue / 65536) % 256,
		mathFloor (numberValue / 16777216) % 256
	)
end

local function writeString (output, data)
	data = tostring (data or "")
	writeUnsigned32 (output, #data)
	output [#output + 1] = data
end

local function readUnsigned8 (reader)
	if reader.Position > reader.Length then
		error ("(ChrononLabs-StreamNet): Decode reached end of stream. Make sure both sides use the same library version and serializer format.")
	end

	local numberValue = stringByte (reader.Data, reader.Position)
	reader.Position   = reader.Position + 1

	return numberValue
end

local function readUnsigned16 (reader)
	local lowByte  = readUnsigned8 (reader)
	local highByte = readUnsigned8 (reader)

	return lowByte + highByte * 256
end

local function readUnsigned32 (reader)
	local byte1 = readUnsigned8 (reader)
	local byte2 = readUnsigned8 (reader)
	local byte3 = readUnsigned8 (reader)
	local byte4 = readUnsigned8 (reader)

	return byte1 + byte2 * 256 + byte3 * 65536 + byte4 * 16777216
end

local function readSigned32 (reader)
	local numberValue = readUnsigned32 (reader)

	if numberValue >= 2147483648 then
		numberValue = numberValue - 4294967296
	end

	return numberValue
end

local function readString (reader)
	local length = readUnsigned32 (reader)

	if length < 0 or length > config.MaximumPayloadBytes then
		error ("(ChrononLabs-StreamNet): Decode string length is invalid. Check for corrupted payloads or mismatched library versions.")
	end

	if reader.Position + length - 1 > reader.Length then
		error ("(ChrononLabs-StreamNet): Decode string exceeds stream. Check for corrupted payloads or mismatched library versions.")
	end

	local data      = stringSub (reader.Data, reader.Position, reader.Position + length - 1)
	reader.Position = reader.Position + length

	return data
end

-- IEEE-754 binary64 packing in Lua since GMod has no FFI or string.pack.
local function writeDouble (output, numberValue)
	numberValue = numberValue or 0

	local sign = 0

	if numberValue < 0 or (numberValue == 0 and 1 / numberValue == -mathHuge) then
		sign        = 1
		numberValue = -numberValue
	end

	local exponent
	local fraction

	if numberValue == 0 then
		exponent = 0
		fraction = 0
	elseif numberValue == mathHuge then
		exponent = 2047
		fraction = 0
	elseif numberValue ~= numberValue then
		exponent = 2047
		fraction = 1
	else
		local mantissa, rawExponent = mathFrexp (numberValue)
		exponent = rawExponent + 1022

		if exponent <= 0 then
			fraction = mathFloor (mantissa * 2 ^ (52 + exponent) + 0.5)
			exponent = 0
		else
			fraction = mathFloor ((mantissa * 2 - 1) * 4503599627370496 + 0.5)
		end

		if fraction == 4503599627370496 then
			fraction = 0
			exponent = exponent + 1

			if exponent >= 2047 then
				exponent = 2047
			end
		end
	end

	local highWord = sign * 2147483648 + exponent * 1048576 + mathFloor (fraction / 4294967296)

	writeUnsigned32 (output, highWord)
	writeUnsigned32 (output, fraction % 4294967296)
end

local function readDouble (reader)
	local highWord = readUnsigned32 (reader)
	local lowWord  = readUnsigned32 (reader)

	local sign     = mathFloor (highWord / 2147483648) % 2
	local exponent = mathFloor (highWord / 1048576) % 2048
	local fraction = (highWord % 1048576) * 4294967296 + lowWord
	local value

	if exponent == 2047 then
		value = fraction == 0 and mathHuge or mathHuge - mathHuge
	elseif exponent == 0 then
		value = fraction == 0 and 0.0 or mathLdexp (fraction, -1074)
	else
		value = mathLdexp (fraction + 4503599627370496, exponent - 1075)
	end

	if sign == 1 then
		value = -value
	end

	return value
end

local writeValue
local readValue

writeValue = function (output, value, depth, seen)
	local valueType = type (value)

	if valueType == "nil" then
		writeUnsigned8 (output, tagNil)
		return
	end

	if valueType == "boolean" then
		writeUnsigned8 (output, value and tagTrue or tagFalse)
		return
	end

	if valueType == "number" then
		if value == mathFloor (value) and value >= -2147483648 and value <= 2147483647 and not (value == 0 and 1 / value == -mathHuge) then
			writeUnsigned8 (output, tagInt32)
			writeUnsigned32 (output, value)
		else
			writeUnsigned8 (output, tagNumber)
			writeDouble (output, value)
		end

		return
	end

	if valueType == "string" then
		writeUnsigned8 (output, tagString)
		writeString (output, value)
		return
	end

	if IsEntity and IsEntity (value) then
		writeUnsigned8 (output, tagEntity)

		local entityIndex = 0
		local creationId  = 0

		if IsValid (value) then
			entityIndex = value:EntIndex ()

			if entityIndex > 0 then
				creationId = value.GetCreationID and value:GetCreationID () or 0
			else
				entityIndex = 0
			end
		end

		writeUnsigned32 (output, entityIndex)
		writeUnsigned32 (output, creationId)
		return
	end

	if isvector and isvector (value) then
		writeUnsigned8 (output, tagVector)
		writeDouble (output, value.x)
		writeDouble (output, value.y)
		writeDouble (output, value.z)
		return
	end

	if isangle and isangle (value) then
		writeUnsigned8 (output, tagAngle)
		writeDouble (output, value.p)
		writeDouble (output, value.y)
		writeDouble (output, value.r)
		return
	end

	if IsColor and IsColor (value) then
		writeUnsigned8 (output, tagColor)
		writeUnsigned8 (output, clamp (mathFloor (value.r or 255), 0, 255))
		writeUnsigned8 (output, clamp (mathFloor (value.g or 255), 0, 255))
		writeUnsigned8 (output, clamp (mathFloor (value.b or 255), 0, 255))
		writeUnsigned8 (output, clamp (mathFloor (value.a or 255), 0, 255))
		return
	end

	if valueType == "table" then
		if depth > config.MaximumTableDepth then
			error ("(ChrononLabs-StreamNet): Serializer table depth limit exceeded. Increase MaximumTableDepth, or flatten the table.")
		end

		if seen [value] then
			error ("(ChrononLabs-StreamNet): Serializer cyclic table. Remove self-references before sending.")
		end

		seen [value] = true

		local pairCount = 0

		for key in pairs (value) do
			pairCount = pairCount + 1

			if pairCount > config.MaximumTablePairs then
				seen [value] = nil
				error ("(ChrononLabs-StreamNet): Serializer table pair limit exceeded. Increase MaximumTablePairs, or send fewer keys.")
			end
		end

		writeUnsigned8 (output, tagTable)
		writeUnsigned32 (output, pairCount)

		for key, pairValue in pairs (value) do
			writeValue (output, key, depth + 1, seen)
			writeValue (output, pairValue, depth + 1, seen)
		end

		seen [value] = nil
		return
	end

	error ("(ChrononLabs-StreamNet): Serializer unsupported type " .. valueType .. ". Send a network-safe value instead, or use SendRaw with your own serializer.")
end

readValue = function (reader, depth)
	if depth > config.MaximumTableDepth then
		error ("(ChrononLabs-StreamNet): Decode table depth limit exceeded. Check MaximumTableDepth and make sure both sides use matching limits.")
	end

	local tag = readUnsigned8 (reader)

	if tag == tagNil then return nil end
	if tag == tagFalse then return false end
	if tag == tagTrue then return true end
	if tag == tagInt32 then return readSigned32 (reader) end
	if tag == tagNumber then return readDouble (reader) end
	if tag == tagString then return readString (reader) end

	if tag == tagVector then
		return Vector (readDouble (reader), readDouble (reader), readDouble (reader))
	end

	if tag == tagAngle then
		return Angle (readDouble (reader), readDouble (reader), readDouble (reader))
	end

	if tag == tagColor then
		return Color (
			readUnsigned8 (reader),
			readUnsigned8 (reader),
			readUnsigned8 (reader),
			readUnsigned8 (reader)
		)
	end

	if tag == tagEntity then
		local entityIndex = readUnsigned32 (reader)
		local creationId  = readUnsigned32 (reader)

		if entityIndex <= 0 or not Entity then
			return NULL
		end

		local ent = Entity (entityIndex)

		if not IsValid (ent) then
			return NULL
		end

		if creationId > 0 and ent.GetCreationID and ent:GetCreationID () ~= creationId then
			return NULL
		end

		return ent
	end

	if tag == tagTable then
		local pairCount = readUnsigned32 (reader)

		if pairCount > config.MaximumTablePairs then
			error ("(ChrononLabs-StreamNet): Decode table pair limit exceeded. Check MaximumTablePairs and make sure both sides use matching limits.")
		end

		local output = {}

		for pairIndex = 1, pairCount do
			local key       = readValue (reader, depth + 1)
			local pairValue = readValue (reader, depth + 1)

			if key ~= nil then
				output [key] = pairValue
			end
		end

		return output
	end

	error ("(ChrononLabs-StreamNet): Decode unknown tag " .. tostring (tag) .. ". Make sure both sides use the same library version and serializer format.")
end

local function encodeArguments (startIndex, ...)
	local argumentCount = select ("#", ...) - startIndex + 1

	if argumentCount < 0 then
		argumentCount = 0
	end

	local output = {}
	writeUnsigned8 (output, 3)
	writeUnsigned16 (output, argumentCount)

	local seen = {}

	for argumentIndex = 1, argumentCount do
		writeValue (output, select (startIndex + argumentIndex - 1, ...), 0, seen)
	end

	return tableConcat (output)
end

local function decodeArguments (data)
	local reader = {
		Data     = data,
		Position = 1,
		Length   = #data
	}

	local version = readUnsigned8 (reader)

	if version ~= 3 then
		error ("(ChrononLabs-StreamNet): Decode serializer version mismatch. Make sure both sides use the same library version.")
	end

	local argumentCount = readUnsigned16 (reader)
	local arguments     = {}

	for argumentIndex = 1, argumentCount do
		arguments [argumentIndex] = readValue (reader, 0)
	end

	if reader.Position <= reader.Length then
		error ("(ChrononLabs-StreamNet): Decode trailing bytes after arguments. Check for corrupted payloads or mismatched library versions.")
	end

	return arguments, argumentCount
end

local function peerKey (peer)
	if CLIENT then return "server" end
	if not isPlayerValue (peer) then return nil end

	return peer:UserID ()
end

local function peerFromKey (key)
	if CLIENT then return nil end

	local ply = library.PlayersByUserId [key]
	if isPlayerValue (ply) then return ply end

	for _, scannedPlayer in playerIterator () do
		if scannedPlayer:UserID () == key then
			library.PlayersByUserId [key] = scannedPlayer
			return scannedPlayer
		end
	end

	library.PlayersByUserId [key] = nil
	return nil
end

local function getOutgoingState (peer)
	local key = peerKey (peer)
	if not key then return nil end

	local state = library.OutgoingStates [key]

	if not state then
		state = {
			Key             = key,
			Peer            = peer,
			Queue           = {},
			ById            = {},
			ActiveTransfers = {},
			Budget          = config.BurstBytes,
			LastTick        = now ()
		}

		library.OutgoingStates [key] = state
	else
		state.Peer = peer
	end

	return state
end

local function readOutgoingState (peer)
	local key = peerKey (peer)
	if not key then return nil, nil end

	return library.OutgoingStates [key], key
end

local function getIncomingBucket (peer)
	local key = peerKey (peer)
	if not key then return nil, nil end

	local bucket = library.IncomingStates [key]

	if not bucket then
		bucket                       = {}
		library.IncomingStates [key] = bucket
	end

	return bucket, key
end

local function canSendToPeer (peer)
	if CLIENT then return true end
	if not isPlayerValue (peer) then return false end
	if not config.QueueUntilClientReady then return true end

	return library.ReadyPlayers [peer:UserID ()] == true
end

local function countTable (targetTable)
	local count = 0

	for key in pairs (targetTable) do
		count = count + 1
	end

	return count
end

local function hasAnyEntry (targetTable)
	return next (targetTable) ~= nil
end

local function getFinishedIncomingBucket (peer)
	local key = peerKey (peer)
	if not key then return nil, nil end

	local bucket = library.FinishedIncoming [key]

	if not bucket then
		bucket                         = {}
		library.FinishedIncoming [key] = bucket
	end

	return bucket, key
end

local function metadataMatchesFinished (finished, payloadMode, name, compressed, rawSize, packedSize, totalChunks, fullChecksum)
	return finished.Mode == payloadMode
		and finished.Name == name
		and finished.Compressed == compressed
		and finished.RawSize == rawSize
		and finished.PackedSize == packedSize
		and finished.TotalChunks == totalChunks
		and finished.Checksum == fullChecksum
end

local function enforceFinishedIncomingCap (bucket)
	local maximum = tonumber (config.MaximumFinishedIncomingPerPeer) or 0

	if maximum < 1 then
		for transferId in pairs (bucket) do
			bucket [transferId] = nil
		end

		return
	end

	while countTable (bucket) > maximum do
		local oldestId
		local oldestExpiresAt

		for transferId, finished in pairs (bucket) do
			local expiresAt = finished.ExpiresAt or 0

			if not oldestExpiresAt or expiresAt < oldestExpiresAt then
				oldestId        = transferId
				oldestExpiresAt = expiresAt
			end
		end

		if not oldestId then
			break
		end

		bucket [oldestId] = nil
	end
end

local function rememberFinishedIncoming (peer, incoming, ok, reason, currentTime)
	if not incoming then return nil end

	local bucket = getFinishedIncomingBucket (peer)
	if not bucket then return nil end

	currentTime = currentTime or now ()

	local finished = {
		Ok              = ok == true,
		Reason          = tostring (reason or ""),
		ExpiresAt       = currentTime + config.FinishedIncomingTtl,
		LastControlSent = 0,
		Mode            = incoming.Mode,
		Name            = incoming.Name,
		LowerName       = incoming.LowerName,
		Compressed      = incoming.Compressed,
		RawSize         = incoming.RawSize,
		PackedSize      = incoming.PackedSize,
		TotalChunks     = incoming.TotalChunks,
		Checksum        = incoming.Checksum
	}

	bucket [incoming.Id] = finished

	enforceFinishedIncomingCap (bucket)

	return finished
end

local function countIncomingByName (bucket, messageLowerName)
	local count = 0

	for transferId, incoming in pairs (bucket) do
		if incoming.LowerName == messageLowerName then
			count = count + 1
		end
	end

	return count
end

local function policyCooldownActive (key, messageLowerName, policy, currentTime)
	if not policy.Cooldown then return false end

	local stateForPeer = library.ReceivePolicyState [key]
	local entry        = stateForPeer and stateForPeer [messageLowerName]

	return entry and currentTime - entry.LastAcceptedAt < policy.Cooldown
end

local function recordPolicyAccepted (key, messageLowerName, policy, currentTime)
	if not policy.Cooldown then return end

	local stateForPeer = library.ReceivePolicyState [key]

	if not stateForPeer then
		stateForPeer                    = {}
		library.ReceivePolicyState [key] = stateForPeer
	end

	local entry = stateForPeer [messageLowerName]

	if not entry then
		entry                           = {}
		stateForPeer [messageLowerName] = entry
	end

	entry.LastAcceptedAt = currentTime
end

local function safeChunkSize (name, wantedChunkSize)
	wantedChunkSize = tonumber (wantedChunkSize) or config.ChunkSize

	local overhead         = 160 + #tostring (name or "")
	local maximumChunkSize = clamp (config.MaximumNetMessageBytes - overhead, minimumChunkSize, 60000)

	return clamp (mathFloor (wantedChunkSize), minimumChunkSize, maximumChunkSize)
end

local function maximumChunksForPackedSize (packedSize)
	return mathMax (1, mathCeil ((tonumber (packedSize) or 0) / minimumChunkSize))
end

local function parsePriority (priority)
	if priority == nil then return 2, "normal" end

	local priorityName = lowerName (priority)

	if priorityName == "high" then
		return 3, "high"
	end

	if priorityName == "normal" or priorityName == "default" then
		return 2, "normal"
	end

	if priorityName == "low" then
		return 1, "low"
	end

	return nil, "(ChrononLabs-StreamNet): Invalid priority. Use high, normal, low, or default."
end

local function copyProfileOptions (source)
	local output = {}

	for key, value in pairs (source or {}) do
		output [key] = value
	end

	return output
end

local function profileByName (profileName)
	local profile = library.Profiles [lowerName (profileName)]

	if not profile then
		error ("(ChrononLabs-StreamNet): Unknown profile '" .. tostring (profileName) .. "'. Define it with DefineProfile before using it.")
	end

	return profile
end

local function resolveProfileOptions (value)
	if value == nil then
		return nil
	end

	if type (value) == "string" then
		return copyProfileOptions (profileByName (value))
	end

	if type (value) ~= "table" then
		error ("(ChrononLabs-StreamNet): Profile/options must be nil, a profile name string, or an options table.")
	end

	local profileName = value.Profile or value.profile

	if profileName == nil then
		return value
	end

	local output = copyProfileOptions (profileByName (profileName))

	for key, optionValue in pairs (value) do
		if key ~= "Profile" and key ~= "profile" then
			output [key] = optionValue
		end
	end

	return output
end

local function normalizeReceivePolicy (policy)
	if policy == nil then
		return {}
	end

	assert (type (policy) == "table", "(ChrononLabs-StreamNet): Receive policy must be a table. Pass a policy table or a callback function.")

	local direction = lowerName (policy.Direction or policy.direction or "any")

	assert (
		direction == "any" or direction == "client_to_server" or direction == "server_to_client",
		"(ChrononLabs-StreamNet): Receive policy Direction is invalid. Use any, client_to_server, or server_to_client."
	)

	local maxBytes = policy.MaxBytes or policy.maxBytes

	if maxBytes ~= nil then
		maxBytes = tonumber (maxBytes)
		assert (maxBytes and maxBytes > 0, "(ChrononLabs-StreamNet): Receive policy MaxBytes is invalid. Use a positive number.")
	end

	local maxInFlight = policy.MaxInFlight or policy.maxInFlight

	if maxInFlight ~= nil then
		maxInFlight = tonumber (maxInFlight)
		assert (maxInFlight and maxInFlight >= 1, "(ChrononLabs-StreamNet): Receive policy MaxInFlight is invalid. Use a positive integer.")
		maxInFlight = mathFloor (maxInFlight)
	end

	local cooldown = policy.Cooldown or policy.cooldown

	if cooldown ~= nil then
		cooldown = tonumber (cooldown)
		assert (cooldown and cooldown >= 0, "(ChrononLabs-StreamNet): Receive policy Cooldown is invalid. Use zero or a positive number.")
	end

	return {
		Direction    = direction,
		MaxBytes     = maxBytes,
		MaxInFlight  = maxInFlight,
		Cooldown     = cooldown,
		RequireReady = policy.RequireReady == true or policy.requireReady == true
	}
end

local function prepareTransferPayload (name, payloadMode, payload, options)
	options = options or {}
	name    = tostring (name or "")

	if name == "" then return false, "(ChrononLabs-StreamNet): Empty message name. Pass a non-empty string message name." end
	if #name > 128 then return false, "(ChrononLabs-StreamNet): Message name too long. Use a message name with 128 characters or fewer." end
	if type (payload) ~= "string" then return false, "(ChrononLabs-StreamNet): Payload must be a string. Use Send for structured values or SendRaw with encoded bytes." end
	if #payload > config.MaximumPayloadBytes then return false, "(ChrononLabs-StreamNet): Payload too large. Reduce or split the payload, or increase MaximumPayloadBytes for trusted transfers." end

	local rawSize        = #payload
	local compressed     = false
	local shouldCompress = options.Compress

	if shouldCompress == nil then
		shouldCompress = config.Compress
	end

	if shouldCompress and rawSize >= (options.CompressAt or config.CompressAt) and utilCompress then
		local compressionOk, compressedPayload = pcall (utilCompress, payload)

		if compressionOk and type (compressedPayload) == "string" and #compressedPayload + 16 < #payload then
			payload    = compressedPayload
			compressed = true
		end
	end

	if #payload > config.MaximumPayloadBytes then
		return false, "(ChrononLabs-StreamNet): Packed payload too large. Reduce or split the payload, or increase MaximumPayloadBytes for trusted transfers."
	end

	local chunkSize   = safeChunkSize (name, options.ChunkSize)
	local totalChunks = mathMax (1, mathCeil (#payload / chunkSize))
	local priority, priorityName = parsePriority (options.Priority or options.priority)

	if not priority then
		return false, priorityName
	end

	return {
		Name                  = name,
		LowerName             = lowerName (name),
		Mode                  = payloadMode,
		Data                  = payload,
		RawSize               = rawSize,
		PackedSize            = #payload,
		Compressed            = compressed,
		Checksum              = crc (payload),
		ChunkSize             = chunkSize,
		TotalChunks           = totalChunks,
		RetryInterval         = tonumber (options.RetryInterval) or config.RetryInterval,
		Timeout               = tonumber (options.Timeout) or config.Timeout,
		MaximumRetries        = tonumber (options.MaximumRetries) or config.MaximumRetries,
		Window                = tonumber (options.Window) or config.Window,
		ReliableData          = options.ReliableData == true,
		Priority              = priority,
		PriorityName          = priorityName,
		Callback              = options.OnComplete or options.onComplete,
		ProgressCallback      = options.OnProgress or options.onProgress,
		ProgressInterval      = mathMax (0, tonumber (options.ProgressInterval) or 0.25)
	}
end

local function buildTransferFromPrepared (prepared, peer)
	local currentTime = now ()

	return {
		Id                    = nextTransferId (),
		Name                  = prepared.Name,
		LowerName             = prepared.LowerName,
		Mode                  = prepared.Mode,
		Peer                  = peer,
		Data                  = prepared.Data,
		RawSize               = prepared.RawSize,
		PackedSize            = prepared.PackedSize,
		Compressed            = prepared.Compressed,
		Checksum              = prepared.Checksum,
		ChunkSize             = prepared.ChunkSize,
		TotalChunks           = prepared.TotalChunks,
		NextSequence          = 1,
		Sent                  = {},
		Retries               = {},
		Acked                 = {},
		InFlight              = {},
		InFlightCount         = 0,
		AckCount              = 0,
		NackQueue             = {},
		NackSeen              = {},
		NackHead              = 1,
		CreatedAt             = currentTime,
		LastProgress          = currentTime,
		RetryInterval         = prepared.RetryInterval,
		Timeout               = prepared.Timeout,
		MaximumRetries        = prepared.MaximumRetries,
		Window                = prepared.Window,
		ReliableData          = prepared.ReliableData,
		Priority              = prepared.Priority,
		PriorityName          = prepared.PriorityName,
		LastScheduledAt       = currentTime,
		Callback              = prepared.Callback,
		ProgressCallback      = prepared.ProgressCallback,
		ProgressInterval      = prepared.ProgressInterval,
		LastProgressCallback  = 0,
		LastProgressAckCount  = -1
	}
end

local function makeTransfer (name, peer, payloadMode, payload, options)
	local prepared, errorMessage = prepareTransferPayload (name, payloadMode, payload, options)

	if not prepared then
		return false, errorMessage
	end

	return buildTransferFromPrepared (prepared, peer)
end

local function enqueueTransfer (name, target, payloadMode, payload, options)
	local state = getOutgoingState (target)
	if not state then return false, "(ChrononLabs-StreamNet): Invalid target. Pass a valid player, player list, nil, or true depending on the send direction." end

	local transfer, errorMessage = makeTransfer (name, target, payloadMode, payload, options)

	if not transfer then
		return false, errorMessage
	end

	state.Queue [#state.Queue + 1] = transfer
	state.ById [transfer.Id]       = transfer

	debugPrint ("queued", transfer.Name, "id", transfer.Id, "chunks", transfer.TotalChunks, "bytes", transfer.PackedSize)

	return transfer.Id, transfer
end

local function enqueuePreparedTransfer (prepared, target)
	local state = getOutgoingState (target)
	if not state then return false, "(ChrononLabs-StreamNet): Invalid target. Pass a valid player, player list, nil, or true depending on the send direction." end

	local transfer = buildTransferFromPrepared (prepared, target)

	state.Queue [#state.Queue + 1] = transfer
	state.ById [transfer.Id]       = transfer

	debugPrint ("queued", transfer.Name, "id", transfer.Id, "chunks", transfer.TotalChunks, "bytes", transfer.PackedSize)

	return transfer.Id, transfer
end

local function sendToTargets (name, target, payloadMode, payload, options)
	if CLIENT then
		local id, result = enqueueTransfer (name, nil, payloadMode, payload, options)
		if not id then return false, result end
		return id
	end

	if target == nil or target == true then
		local targets = {}

		for _, ply in playerIterator () do
			if isPlayerValue (ply) then
				targets [#targets + 1] = ply
			end
		end

		if #targets == 0 then
			return false, "(ChrononLabs-StreamNet): No valid targets. Pass at least one valid player target."
		end

		local prepared, errorMessage = prepareTransferPayload (name, payloadMode, payload, options)

		if not prepared then
			return false, errorMessage
		end

		local ids = {}

		for targetIndex, ply in ipairs (targets) do
			local id, result = enqueuePreparedTransfer (prepared, ply)

			if not id then
				return false, result
			end

			ids [#ids + 1] = id
		end

		if #ids == 1 then
			return ids [1]
		end

		return ids
	end

	if isPlayerValue (target) then
		local id, result = enqueueTransfer (name, target, payloadMode, payload, options)
		if not id then return false, result end
		return id
	end

	if type (target) == "table" then
		local targets = {}

		for key, value in pairs (target) do
			local ply = nil

			if isPlayerValue (value) then
				ply = value
			elseif isPlayerValue (key) then
				ply = key
			end

			if ply then
				targets [#targets + 1] = ply
			end
		end

		if #targets == 0 then
			return false, "(ChrononLabs-StreamNet): No valid targets. Pass at least one valid player target."
		end

		local prepared, errorMessage = prepareTransferPayload (name, payloadMode, payload, options)

		if not prepared then
			return false, errorMessage
		end

		local ids = {}

		for targetIndex, ply in ipairs (targets) do
			local id, result = enqueuePreparedTransfer (prepared, ply)

			if not id then
				return false, result
			end

			ids [#ids + 1] = id
		end

		if #ids == 1 then
			return ids [1]
		end

		return ids
	end

	return false, "(ChrononLabs-StreamNet): No valid targets. Pass at least one valid player target."
end

local function requestChannelName (name)
	return requestNamePrefix .. lowerName (name)
end

local function validateRequestName (name)
	if type (name) ~= "string" then
		return false, "(ChrononLabs-StreamNet): Request name must be a string. Pass a non-empty request name as the first argument."
	end

	if name == "" then
		return false, "(ChrononLabs-StreamNet): Empty request name. Pass a non-empty request name."
	end

	if isReservedMessageName (name) then
		return false, "(ChrononLabs-StreamNet): Reserved request name. Names starting with '" .. internalNamePrefix .. "' are used internally."
	end

	if #requestChannelName (name) > 128 then
		return false, "(ChrononLabs-StreamNet): Request name too long. Use a shorter request name."
	end

	return true
end

local function registerInternalReceive (name, policy, callback)
	local messageLowerName = lowerName (name)

	library.Handlers [messageLowerName]        = callback
	library.ReceivePolicies [messageLowerName] = normalizeReceivePolicy (policy)
end

local function finishPendingRequest (requestId, ok, ...)
	local pending = library.PendingRequests [requestId]

	if not pending or pending.Consumed then
		return false
	end

	pending.Consumed                   = true
	library.PendingRequests [requestId] = nil

	local callbackOk, callbackError = pcall (pending.Callback, ok == true, ...)

	if not callbackOk then
		ErrorNoHalt ("(ChrononLabs-StreamNet): Request callback error: " .. tostring (callbackError) .. ". Fix the Request callback for this message.\n")
	end

	return true
end

local function failPendingRequestsForPeer (peer, reason)
	local requestIds = {}

	for requestId, pending in pairs (library.PendingRequests) do
		if pending.Peer == peer then
			requestIds [#requestIds + 1] = requestId
		end
	end

	for requestIndex, requestId in ipairs (requestIds) do
		finishPendingRequest (requestId, false, reason)
	end
end

local function onInternalResponse (...)
	local peer
	local requestId
	local ok
	local replyStartIndex

	if SERVER then
		peer            = select (1, ...)
		requestId       = select (2, ...)
		ok              = select (3, ...)
		replyStartIndex = 4
	else
		requestId       = select (1, ...)
		ok              = select (2, ...)
		replyStartIndex = 3
	end

	if type (requestId) ~= "number" or type (ok) ~= "boolean" then return end

	requestId = mathFloor (requestId)

	local pending = library.PendingRequests [requestId]
	if not pending then return end

	if SERVER and pending.Peer ~= peer then return end

	finishPendingRequest (requestId, ok, select (replyStartIndex, ...))
end

local function installResponseReceiver ()
	if library.ResponseReceiverInstalled then return end

	local policy = {
		Direction = "any"
	}

	if config.ResponseMaxBytes ~= nil then
		policy.MaxBytes = config.ResponseMaxBytes
	end

	registerInternalReceive (responseName, policy, onInternalResponse)

	library.ResponseReceiverInstalled = true
end

local function requestOptions (options)
	local resolvedOptions = resolveProfileOptions (options) or {}

	if resolvedOptions.OnComplete ~= nil or resolvedOptions.onComplete ~= nil then
		return nil, "(ChrononLabs-StreamNet): Request options reserve OnComplete for request delivery tracking. Use the Request callback for the response result."
	end

	local timeout = resolvedOptions.RequestTimeout or resolvedOptions.requestTimeout or resolvedOptions.Timeout or resolvedOptions.timeout
	timeout       = tonumber (timeout) or tonumber (config.RequestTimeout) or 15

	if not timeout or timeout <= 0 then
		return nil, "(ChrononLabs-StreamNet): Request Timeout must be a positive number."
	end

	local sendOptions = copyProfileOptions (resolvedOptions)

	sendOptions.RequestTimeout = nil
	sendOptions.requestTimeout = nil
	sendOptions.Timeout        = nil
	sendOptions.timeout        = nil

	return sendOptions, nil, timeout
end

local function sendInternalArguments (name, peer, options, ...)
	local payload = encodeArguments (1, ...)

	return enqueueTransfer (name, peer, modeArguments, payload, options)
end

local function sendInternalResponse (peer, requestId, ok, ...)
	return sendInternalArguments (responseName, peer, nil, requestId, ok == true, ...)
end

local function startRequest (name, peer, requestData, options, callback)
	local valid, errorMessage = validateRequestName (name)
	if not valid then return false, errorMessage end

	assert (type (callback) == "function", "(ChrononLabs-StreamNet): Request callback must be a function. Pass a function as the last argument.")

	if SERVER and not isPlayerValue (peer) then
		return false, "(ChrononLabs-StreamNet): Request target must be exactly one valid player."
	end

	installResponseReceiver ()

	local sendOptions, optionsError, timeout = requestOptions (options)

	if not sendOptions then
		return false, optionsError
	end

	local channel   = requestChannelName (name)
	local requestId = nextRequestId ()
	local payload   = encodeArguments (1, requestId, requestData)
	local expiresAt = now () + timeout

	library.PendingRequests [requestId] = {
		Id        = requestId,
		Peer      = peer,
		ExpiresAt = expiresAt,
		Consumed  = false,
		Callback  = callback
	}

	sendOptions.OnComplete = function (ok, reason)
		if ok then return end

		reason = tostring (reason or "")
		if reason == "" then
			reason = "(ChrononLabs-StreamNet): Request transport failed."
		end

		finishPendingRequest (requestId, false, reason)
	end

	local transferId, result = enqueueTransfer (channel, peer, modeArguments, payload, sendOptions)

	if not transferId then
		finishPendingRequest (requestId, false, result)
		return false, result
	end

	return requestId, transferId
end

local function sweepPendingRequests (currentTime)
	if not next (library.PendingRequests) then return end

	local requestIds = {}

	for requestId, pending in pairs (library.PendingRequests) do
		if currentTime >= (pending.ExpiresAt or 0) then
			requestIds [#requestIds + 1] = requestId
		end
	end

	for requestIndex, requestId in ipairs (requestIds) do
		finishPendingRequest (requestId, false, "timeout")
	end
end

local function respondHandler (name, callback)
	return function (...)
		local peer
		local requestId
		local requestData

		if SERVER then
			peer        = select (1, ...)
			requestId   = select (2, ...)
			requestData = select (3, ...)
		else
			requestId   = select (1, ...)
			requestData = select (2, ...)
		end

		if type (requestId) ~= "number" then return end

		requestId = mathFloor (requestId)

		local replied = false

		local function reply (ok, ...)
			if replied then return false, "(ChrononLabs-StreamNet): Request already replied." end

			local id, result = sendInternalResponse (peer, requestId, ok == true, ...)

			replied = true

			return id, result
		end

		local callbackOk, callbackError

		if SERVER then
			callbackOk, callbackError = pcall (callback, peer, requestData, reply)
		else
			callbackOk, callbackError = pcall (callback, requestData, reply)
		end

		if not callbackOk then
			ErrorNoHalt ("(ChrononLabs-StreamNet): Responder error for " .. tostring (name) .. ": " .. tostring (callbackError) .. ". Fix the Respond callback for this request.\n")

			if not replied then
				reply (false, "responder error")
			end
		end
	end
end

local function queueControlPacket (root, peer, transferId, sequence)
	local key = peerKey (peer)
	if not key then return end

	local byId = root [key]

	if not byId then
		byId       = {}
		root [key] = byId
	end

	local sequenceSet = byId [transferId]

	if not sequenceSet then
		sequenceSet       = {}
		byId [transferId] = sequenceSet
	end

	sequenceSet [sequence] = true
end

local function queueAck (peer, transferId, sequence)
	queueControlPacket (library.AckPending, peer, transferId, sequence)
end

local function queueNack (peer, transferId, sequence)
	queueControlPacket (library.NackPending, peer, transferId, sequence)
end

local function takeSequences (sequenceSet, maximumCount)
	local output = {}

	for sequence in pairs (sequenceSet) do
		output [#output + 1]   = sequence
		sequenceSet [sequence] = nil

		if #output >= maximumCount then
			break
		end
	end

	return output
end

local function sendSequenceList (peer, packetKind, transferId, sequences)
	if #sequences == 0 then return false end

	startPacket (packetKind, false)
	writeNetUnsigned32 (transferId)
	netWriteUInt (#sequences, 8)

	for sequenceIndex = 1, #sequences do
		writeNetUnsigned32 (sequences [sequenceIndex])
	end

	return sendCurrentPacket (peer)
end

local function sendCancel (peer, transferId, reason)
	startPacket (packetCancel, false)
	writeNetUnsigned32 (transferId)
	netWriteString (tostring (reason or "(ChrononLabs-StreamNet): Cancel. The remote side cancelled this transfer."))
	sendCurrentPacket (peer)
end

local function sendComplete (peer, transferId, ok, reason)
	startPacket (packetComplete, false)
	writeNetUnsigned32 (transferId)
	netWriteBool (ok == true)
	netWriteString (tostring (reason or ""))
	sendCurrentPacket (peer)
end

local function emitProgress (transfer, currentTime, force)
	if not transfer.ProgressCallback then return end

	if not force then
		if transfer.AckCount == transfer.LastProgressAckCount then return end
		if currentTime - transfer.LastProgressCallback < transfer.ProgressInterval then return end
	end

	transfer.LastProgressCallback = currentTime
	transfer.LastProgressAckCount = transfer.AckCount

	local callbackOk, callbackError = pcall (transfer.ProgressCallback, transfer)

	if not callbackOk then
		ErrorNoHalt ("(ChrononLabs-StreamNet): OnProgress error: " .. tostring (callbackError) .. ". Fix the OnProgress callback for this transfer.\n")
	end
end

local function completeTransfer (state, transfer, ok, reason)
	if transfer.Done then return end

	transfer.Done            = true
	state.ById [transfer.Id] = nil

	if ok then
		library.Metrics.Completed = library.Metrics.Completed + 1
	else
		library.Metrics.Failed = library.Metrics.Failed + 1
	end

	emitProgress (transfer, now (), true)

	if transfer.Callback then
		local callbackOk, callbackError = pcall (transfer.Callback, ok, reason or "", transfer)

		if not callbackOk then
			ErrorNoHalt ("(ChrononLabs-StreamNet): OnComplete error: " .. tostring (callbackError) .. ". Fix the OnComplete callback for this transfer.\n")
		end
	end
end

local function pushNack (transfer, sequence)
	if sequence < 1 or sequence > transfer.TotalChunks then return end
	if transfer.Acked [sequence] then return end
	if transfer.NackSeen [sequence] then return end

	transfer.NackSeen [sequence]                 = true
	transfer.NackQueue [#transfer.NackQueue + 1] = sequence
end

local function popNack (transfer)
	while transfer.NackHead <= #transfer.NackQueue do
		local sequence               = transfer.NackQueue [transfer.NackHead]
		transfer.NackHead            = transfer.NackHead + 1
		transfer.NackSeen [sequence] = nil

		if not transfer.Acked [sequence] then
			return sequence
		end
	end

	return nil
end

local function timedOutSequence (transfer, currentTime)
	for sequence, sentAt in pairs (transfer.Sent) do
		if not transfer.Acked [sequence] and currentTime - sentAt >= transfer.RetryInterval then
			return sequence
		end
	end

	return nil
end

local function sendChunk (state, transfer, sequence, retry)
	local startPosition = (sequence - 1) * transfer.ChunkSize + 1
	local endPosition   = mathMin (sequence * transfer.ChunkSize, transfer.PackedSize)
	local chunk         = stringSub (transfer.Data, startPosition, endPosition)
	local chunkLength   = #chunk

	startPacket 		(packetData, not transfer.ReliableData)
	writeNetUnsigned32 	(transfer.Id)
	netWriteUInt 		(transfer.Mode, 2)
	netWriteString 		(transfer.Name)
	netWriteBool 		(transfer.Compressed)
	writeNetUnsigned32 	(transfer.RawSize)
	writeNetUnsigned32 	(transfer.PackedSize)
	writeNetUnsigned32 	(transfer.TotalChunks)
	writeNetUnsigned32 	(sequence)
	netWriteString 		(transfer.Checksum)
	netWriteString 		(crc (chunk))
	netWriteUInt 		(chunkLength, 16)

	if chunkLength > 0 then
		netWriteData (chunk, chunkLength)
	end

	if sendCurrentPacket (transfer.Peer) then
		local currentTime        = now ()
		transfer.Sent [sequence] = currentTime

		if retry then
			transfer.Retries [sequence] = (transfer.Retries [sequence] or 0) + 1
			library.Metrics.Retransmits = library.Metrics.Retransmits + 1
		elseif not transfer.InFlight [sequence] then
			transfer.InFlight [sequence] = true
			transfer.InFlightCount       = transfer.InFlightCount + 1
		end

		library.Metrics.SentChunks = library.Metrics.SentChunks + 1
		library.Metrics.SentBytes  = library.Metrics.SentBytes + chunkLength

		return true, chunkLength + 160 + #transfer.Name
	end

	return false, 0
end

local function pumpTransfer (state, transfer, currentTime)
	if not canSendToPeer (transfer.Peer) then
		return false
	end

	if currentTime - transfer.LastProgress > transfer.Timeout then
		sendCancel (transfer.Peer, transfer.Id, "(ChrononLabs-StreamNet): Sender timeout. Increase Timeout, reduce payload size, or lower pacing pressure.")
		completeTransfer (state, transfer, false, "(ChrononLabs-StreamNet): Timeout. Increase Timeout, reduce payload size, or lower pacing pressure.")
		return false
	end

	local sentPackets = 0

	while sentPackets < config.MaximumPacketsPerThink do
		local sequence = popNack (transfer)
		local retry    = false

		if sequence then
			retry = true
		else
			sequence = timedOutSequence (transfer, currentTime)

			if sequence then
				retry = true
			elseif transfer.InFlightCount < transfer.Window and transfer.NextSequence <= transfer.TotalChunks then
				sequence              = transfer.NextSequence
				transfer.NextSequence = transfer.NextSequence + 1
			else
				break
			end
		end

		if transfer.Acked [sequence] then
			break
		end

		local retryCount = transfer.Retries [sequence] or 0

		if retry and retryCount >= transfer.MaximumRetries then
			sendCancel (transfer.Peer, transfer.Id, "(ChrononLabs-StreamNet): Maximum retries reached. Increase MaximumRetries or RetryInterval, or reduce transfer pressure.")
			completeTransfer (state, transfer, false, "(ChrononLabs-StreamNet): Maximum retries reached. Increase MaximumRetries or RetryInterval, or reduce transfer pressure.")
			return false
		end

		local startPosition = (sequence - 1) * transfer.ChunkSize + 1
		local endPosition   = mathMin (sequence * transfer.ChunkSize, transfer.PackedSize)
		local chunkLength   = mathMax (0, endPosition - startPosition + 1)
		local cost          = chunkLength + 160 + #transfer.Name

		if state.Budget < cost and sentPackets > 0 then
			break
		end

		if state.Budget < 512 and sentPackets == 0 then
			break
		end

		local sent, usedBytes = sendChunk (state, transfer, sequence, retry)

		if not sent then
			completeTransfer (state, transfer, false, "(ChrononLabs-StreamNet): Send failed. Check the target and avoid sending while the peer is disconnecting.")
			return false
		end

		state.Budget = mathMax (0, state.Budget - usedBytes)
		sentPackets  = sentPackets + 1
	end

	emitProgress (transfer, currentTime, false)

	return sentPackets > 0
end

local function compareScheduledTransfers (leftTransfer, rightTransfer)
	if leftTransfer.SortPriority ~= rightTransfer.SortPriority then
		return leftTransfer.SortPriority > rightTransfer.SortPriority
	end

	return leftTransfer.CreatedAt < rightTransfer.CreatedAt
end

local function flushOutgoing (currentTime)
	for key, state in pairs (library.OutgoingStates) do
		local valid = true

		if SERVER then
			valid = isPlayerValue (state.Peer)
		end

		if not valid then
			library.OutgoingStates [key] = nil
		else
			local deltaTime = currentTime - (state.LastTick or currentTime)
			state.LastTick  = currentTime
			state.Budget    = mathMin (config.BurstBytes, (state.Budget or 0) + config.BytesPerSecond * mathMax (0, deltaTime))

			local writeIndex  = 1
			local queueLength = #state.Queue

			for readIndex = 1, queueLength do
				local transfer = state.Queue [readIndex]

				if not transfer.Done then
					state.Queue [writeIndex] = transfer
					writeIndex = writeIndex + 1
				end
			end

			for clearIndex = queueLength, writeIndex, -1 do
				state.Queue [clearIndex] = nil
			end

			local activeTransfers = state.ActiveTransfers or {}
			state.ActiveTransfers = activeTransfers

			local agingInterval = mathMax (0.001, tonumber (config.PriorityAgingInterval) or 2)
			local activeCount   = 0

			for transferIndex = 1, #state.Queue do
				local transfer = state.Queue [transferIndex]

				transfer.SortPriority = transfer.Priority + mathMin (2, (currentTime - transfer.LastScheduledAt) / agingInterval)

				activeCount = activeCount + 1
				activeTransfers [activeCount] = transfer
			end

			for clearIndex = #activeTransfers, activeCount + 1, -1 do
				activeTransfers [clearIndex] = nil
			end

			if activeCount > 1 then
				tableSort (activeTransfers, compareScheduledTransfers)
			end

			for transferIndex = 1, activeCount do
				local transfer = activeTransfers [transferIndex]

				if pumpTransfer (state, transfer, currentTime) then
					transfer.LastScheduledAt = currentTime
				end
			end
		end
	end
end

local function failIncoming (peer, bucket, incoming, reason)
	if bucket then
		bucket [incoming.Id] = nil
	end

	rememberFinishedIncoming (peer, incoming, false, reason)
	sendCancel (peer, incoming.Id, reason)
	library.Metrics.Failed = library.Metrics.Failed + 1
end

local function deliverIncoming (peer, bucket, incoming)
	local packedPayload = tableConcat (incoming.Chunks, "", 1, incoming.TotalChunks)

	if #packedPayload ~= incoming.PackedSize then
		failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Assembled size mismatch. Check for corrupted chunks or mismatched library versions.")
		return
	end

	if crc (packedPayload) ~= incoming.Checksum then
		failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Assembled checksum mismatch. Check for corrupted chunks or mismatched library versions.")
		return
	end

	local payload = packedPayload

	if incoming.Compressed then
		if not utilDecompress then
			failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Decompressor unavailable. Disable compression or run in an environment with util.Decompress.")
			return
		end

		local decompressOk, decompressedPayload = pcall (utilDecompress, packedPayload, incoming.RawSize)

		if not decompressOk or type (decompressedPayload) ~= "string" then
			failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Decompression failed. Check for corrupted payloads or mismatched compression settings.")
			return
		end

		payload = decompressedPayload
	end

	if #payload ~= incoming.RawSize then
		failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Raw size mismatch. Check for corrupted payloads or mismatched library versions.")
		return
	end

	local callback = library.Handlers [incoming.LowerName]
	local arguments
	local argumentCount

	if callback then
		if incoming.Mode ~= modeRaw then
			local decodeOk

			decodeOk, arguments, argumentCount = pcall (decodeArguments, payload)

			if not decodeOk then
				failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Decode failed. Make sure both sides use the same serializer format.")
				return
			end
		end
	end

	bucket [incoming.Id] = nil
	rememberFinishedIncoming (peer, incoming, true, "ok")

	if callback then
		if incoming.Mode == modeRaw then
			local callbackOk, callbackError

			if SERVER then
				callbackOk, callbackError = pcall (callback, peer, payload)
			else
				callbackOk, callbackError = pcall (callback, payload)
			end

			if not callbackOk then
				ErrorNoHalt ("(ChrononLabs-StreamNet): Handler error for " .. incoming.Name .. ": " .. tostring (callbackError) .. ". Fix the Receive callback for this message.\n")
			end
		else
			local callbackOk, callbackError

			if SERVER then
				callbackOk, callbackError = pcall (callback, peer, tableUnpack (arguments, 1, argumentCount))
			else
				callbackOk, callbackError = pcall (callback, tableUnpack (arguments, 1, argumentCount))
			end

			if not callbackOk then
				ErrorNoHalt ("(ChrononLabs-StreamNet): Handler error for " .. incoming.Name .. ": " .. tostring (callbackError) .. ". Fix the Receive callback for this message.\n")
			end
		end
	end

	sendComplete (peer, incoming.Id, true, "ok")
	library.Metrics.Completed = library.Metrics.Completed + 1
end

local function onDataPacket (peer)
	local currentTime   = now ()
	local transferId    = readNetUnsigned32 ()
	local payloadMode   = netReadUInt 		(2)
	local name          = netReadString 	()
	local compressed    = netReadBool 		()
	local rawSize       = readNetUnsigned32 ()
	local packedSize    = readNetUnsigned32 ()
	local totalChunks   = readNetUnsigned32 ()
	local sequence      = readNetUnsigned32 ()
	local fullChecksum  = netReadString 	()
	local chunkChecksum = netReadString 	()
	local chunkLength   = netReadUInt 		(16)
	local chunk         = ""

	if chunkLength > 0 then
		chunk = netReadData (chunkLength)
	end

	library.Metrics.ReceivedChunks = library.Metrics.ReceivedChunks + 1
	library.Metrics.ReceivedBytes  = library.Metrics.ReceivedBytes + chunkLength

	if name == "" or #name > 128 then return end
	if payloadMode ~= modeArguments and payloadMode ~= modeRaw then return end
	if rawSize > config.MaximumPayloadBytes or packedSize > config.MaximumPayloadBytes then return end
	if totalChunks < 1 or totalChunks > maximumChunksForPackedSize (packedSize) then return end
	if sequence < 1 or sequence > totalChunks then return end
	if #chunk ~= chunkLength then return end

	if packedSize == 0 then
		if totalChunks ~= 1 or sequence ~= 1 or chunkLength ~= 0 then return end
	else
		if chunkLength == 0 then return end
		if chunkLength > packedSize then return end
	end

	if crc (chunk) ~= chunkChecksum then
		queueNack (peer, transferId, sequence)
		return
	end

	local key = peerKey (peer)
	if not key then return end

	local finishedBucket = library.FinishedIncoming [key]
	local finished       = finishedBucket and finishedBucket [transferId]

	if finished and metadataMatchesFinished (finished, payloadMode, name, compressed, rawSize, packedSize, totalChunks, fullChecksum) then
		if finished.Ok then
			queueAck (peer, transferId, sequence)

			if currentTime - (finished.LastControlSent or 0) >= config.FinishedControlResendInterval then
				sendComplete (peer, transferId, true, "ok")
				finished.LastControlSent = currentTime
			end
		elseif currentTime - (finished.LastControlSent or 0) >= config.FinishedControlResendInterval then
			sendCancel (peer, transferId, finished.Reason ~= "" and finished.Reason or "(ChrononLabs-StreamNet): Transfer failed. Retry later or reduce transfer pressure.")
			finished.LastControlSent = currentTime
		end

		return
	end

	local messageLowerName = lowerName (name)
	local policy           = library.ReceivePolicies [messageLowerName]
	local bucket           = library.IncomingStates [key]
	local incoming         = bucket and bucket [transferId]

	if not library.Handlers [messageLowerName] then
		if incoming then
			failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): No receiver registered for this message.")
		else
			sendCancel (peer, transferId, "(ChrononLabs-StreamNet): No receiver registered for this message.")
		end

		return
	end

	if not bucket then
		bucket                       = {}
		library.IncomingStates [key] = bucket
	end

	if not incoming then
		if countTable (bucket) >= config.MaximumIncomingTransfersPerPeer then
			sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Too many incoming transfers. Increase MaximumIncomingTransfersPerPeer or send fewer concurrent transfers.")
			return
		end

		if policy then
			if (SERVER and policy.Direction == "server_to_client") or (CLIENT and policy.Direction == "client_to_server") then
				sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Receive policy direction rejected this transfer. Verify the message Direction matches the sender side.")
				return
			end

			if SERVER and policy.RequireReady and isPlayerValue (peer) and library.ReadyPlayers [peer:UserID ()] ~= true then
				sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Receive policy requires the client to be ready. Wait until the client finishes joining before sending.")
				return
			end

			if policy.MaxBytes and rawSize > policy.MaxBytes then
				sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Receive policy byte limit exceeded. Increase MaxBytes or send less data.")
				return
			end

			if policy.MaxInFlight and countIncomingByName (bucket, messageLowerName) >= policy.MaxInFlight then
				sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Receive policy in-flight limit exceeded. Increase MaxInFlight or wait for the previous transfer to finish.")
				return
			end

			if policyCooldownActive (key, messageLowerName, policy, currentTime) then
				sendCancel (peer, transferId, "(ChrononLabs-StreamNet): Receive policy cooldown active. Wait before sending this message again.")
				return
			end
		end

		incoming = {
			Id            = transferId,
			Mode          = payloadMode,
			Name          = name,
			LowerName     = messageLowerName,
			Compressed    = compressed,
			RawSize       = rawSize,
			PackedSize    = packedSize,
			TotalChunks   = totalChunks,
			Checksum      = fullChecksum,
			Chunks        = {},
			Received      = 0,
			ReceivedBytes = 0,
			CreatedAt     = currentTime,
			UpdatedAt     = currentTime,
			NextNack      = currentTime + config.NackInterval
		}

		bucket [transferId] = incoming
		recordPolicyAccepted (key, messageLowerName, policy or {}, currentTime)
	else
		if incoming.Mode ~= payloadMode or incoming.Name ~= name or incoming.Compressed ~= compressed or incoming.RawSize ~= rawSize or incoming.PackedSize ~= packedSize or incoming.TotalChunks ~= totalChunks or incoming.Checksum ~= fullChecksum then
			failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Metadata mismatch. Make sure transfer IDs are not reused with different metadata.")
			return
		end
	end

	incoming.UpdatedAt = currentTime

	if not incoming.Chunks [sequence] then
		if incoming.ReceivedBytes + chunkLength > incoming.PackedSize then
			failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Received bytes exceed declared packed size. Check for corrupted chunks or mismatched library versions.")
			return
		end

		incoming.Chunks [sequence] = chunk
		incoming.Received          = incoming.Received + 1
		incoming.ReceivedBytes     = incoming.ReceivedBytes + chunkLength
	end

	queueAck (peer, transferId, sequence)

	if incoming.Received == incoming.TotalChunks then
		deliverIncoming (peer, bucket, incoming)
	end
end

local function onAckPacket (peer)
	local transferId    = readNetUnsigned32 ()
	local sequenceCount = netReadUInt (8)
	local state         = getOutgoingState (peer)
	local transfer      = state and state.ById [transferId]

	for sequenceIndex = 1, sequenceCount do
		local sequence = readNetUnsigned32 ()

		if transfer and sequence >= 1 and sequence <= transfer.TotalChunks and not transfer.Acked [sequence] then
			transfer.Acked [sequence] = true
			transfer.AckCount         = transfer.AckCount + 1
			transfer.LastProgress     = now ()

			if transfer.InFlight [sequence] then
				transfer.InFlight [sequence] = nil
				transfer.InFlightCount       = mathMax (0, transfer.InFlightCount - 1)
			end
		end
	end
end

local function onNackPacket (peer)
	local transferId    = readNetUnsigned32 ()
	local sequenceCount = netReadUInt (8)
	local state         = getOutgoingState (peer)
	local transfer      = state and state.ById [transferId]

	for sequenceIndex = 1, sequenceCount do
		local sequence = readNetUnsigned32 ()

		if transfer then
			pushNack (transfer, sequence)
		end
	end
end

local function onCompletePacket (peer)
	local transferId = readNetUnsigned32 ()
	local ok         = netReadBool ()
	local reason     = netReadString ()
	local state      = getOutgoingState (peer)
	local transfer   = state and state.ById [transferId]

	if transfer then
		if ok then
			for sequence = 1, transfer.TotalChunks do
				transfer.Acked [sequence] = true
			end

			transfer.AckCount       = transfer.TotalChunks
			transfer.InFlight       = {}
			transfer.InFlightCount  = 0
			transfer.LastProgress   = now ()
		end

		completeTransfer (state, transfer, ok, reason)
	end
end

local function onCancelPacket (peer)
	local transferId = readNetUnsigned32 ()
	local reason     = netReadString ()

	local state    = getOutgoingState (peer)
	local transfer = state and state.ById [transferId]

	if transfer then
		completeTransfer (state, transfer, false, reason ~= "" and reason or "(ChrononLabs-StreamNet): Remote cancel. The remote side cancelled this transfer.")
	end

	local bucket = getIncomingBucket (peer)

	if bucket then
		local incoming = bucket [transferId]

		if incoming then
			rememberFinishedIncoming (peer, incoming, false, reason ~= "" and reason or "(ChrononLabs-StreamNet): Remote cancel. The remote side cancelled this transfer.")
		end

		bucket [transferId] = nil
	end
end

local function onReadyPacket (peer)
	if SERVER and isPlayerValue (peer) then
		library.ReadyPlayers [peer:UserID ()] = true
	end
end

netReceive (channelName, function (length, ply)
	local peer       = CLIENT and nil or ply
	local packetKind = netReadUInt (4)
	local version    = netReadUInt (4)

	if version ~= protocolVersion then return end

	if packetKind == packetData then
		onDataPacket (peer)
	elseif packetKind == packetAck then
		onAckPacket (peer)
	elseif packetKind == packetNack then
		onNackPacket (peer)
	elseif packetKind == packetComplete then
		onCompletePacket (peer)
	elseif packetKind == packetCancel then
		onCancelPacket (peer)
	elseif packetKind == packetReady then
		onReadyPacket (peer)
	end
end)

local function flushControlRoot (root, packetKind, batchSize)
	for key, byId in pairs (root) do
		local peer  = peerFromKey (key)
		local valid = CLIENT or isPlayerValue (peer)

		if not valid then
			root [key] = nil
		else
			for transferId, sequenceSet in pairs (byId) do
				local sequences = takeSequences (sequenceSet, batchSize)

				if #sequences > 0 then
					sendSequenceList (peer, packetKind, transferId, sequences)
				end

				if not hasAnyEntry (sequenceSet) then
					byId [transferId] = nil
				end
			end

			if not hasAnyEntry (byId) then
				root [key] = nil
			end
		end
	end
end

local function flushIncomingMaintenance (currentTime)
	for key, bucket in pairs (library.IncomingStates) do
		local peer  = peerFromKey (key)
		local valid = CLIENT or isPlayerValue (peer)

		if not valid then
			library.IncomingStates [key] = nil
		else
			for transferId, incoming in pairs (bucket) do
				if currentTime - incoming.UpdatedAt > config.Timeout then
					failIncoming (peer, bucket, incoming, "(ChrononLabs-StreamNet): Incoming timeout. Increase Timeout, reduce payload size, or lower pacing pressure.")
				elseif currentTime >= incoming.NextNack and incoming.Received < incoming.TotalChunks then
					local emitted = 0

					for sequence = 1, incoming.TotalChunks do
						if not incoming.Chunks [sequence] then
							queueNack (peer, transferId, sequence)
							emitted = emitted + 1

							if emitted >= config.NackBatch then
								break
							end
						end
					end

					incoming.NextNack = currentTime + config.NackInterval
				end
			end
		end
	end
end

local function sweepFinishedIncoming (currentTime)
	for key, bucket in pairs (library.FinishedIncoming) do
		local peer  = peerFromKey (key)
		local valid = CLIENT or isPlayerValue (peer)

		if not valid then
			library.FinishedIncoming [key] = nil
		else
			for transferId, finished in pairs (bucket) do
				if currentTime >= finished.ExpiresAt then
					bucket [transferId] = nil
				end
			end

			enforceFinishedIncomingCap (bucket)

			if not hasAnyEntry (bucket) then
				library.FinishedIncoming [key] = nil
			end
		end
	end
end

local nextControlFlush = 0

function library.Tick ()
	local currentTime = now ()

	flushOutgoing (currentTime)
	flushIncomingMaintenance (currentTime)
	sweepFinishedIncoming (currentTime)
	sweepPendingRequests (currentTime)

	if currentTime >= nextControlFlush then
		flushControlRoot (library.AckPending, packetAck, config.AckBatch)
		flushControlRoot (library.NackPending, packetNack, config.NackBatch)
		nextControlFlush = currentTime + config.AckInterval
	end
end

local function findOutgoingTransfer (transferId, peer)
	transferId = tonumber (transferId)
	if not transferId then return nil, nil end

	local state = readOutgoingState (peer)
	if not state then return nil, nil end

	local transfer = state.ById [transferId]
	if transfer then return transfer, state end

	for transferIndex, queuedTransfer in ipairs (state.Queue) do
		if queuedTransfer.Id == transferId then
			return queuedTransfer, state
		end
	end

	return nil, state
end

function library.DefineProfile (name, options)
	assert (type (name) == "string" and name ~= "", "(ChrononLabs-StreamNet): Profile name must be a non-empty string.")
	assert (type (options) == "table", "(ChrononLabs-StreamNet): Profile options must be a table.")

	library.Profiles [lowerName (name)] = copyProfileOptions (options)

	return library
end

function library.Receive (name, policyOrCallback, maybeCallback)
	assert (type (name) == "string", "(ChrononLabs-StreamNet): Receive name must be a string. Pass the registered message name as the first argument.")

	local valid, errorMessage = validatePublicMessageName (name)
	assert (valid, errorMessage)

	local policy
	local callback

	if type (policyOrCallback) == "function" and maybeCallback == nil then
		callback = policyOrCallback
		policy   = nil
	elseif policyOrCallback == nil and type (maybeCallback) == "function" then
		callback = maybeCallback
		policy   = nil
	else
		policy   = normalizeReceivePolicy (resolveProfileOptions (policyOrCallback))
		callback = maybeCallback
	end

	assert (type (callback) == "function", "(ChrononLabs-StreamNet): Receive callback must be a function. Pass a function as the second argument.")

	local messageLowerName = lowerName (name)

	library.Handlers [messageLowerName]        = callback
	library.ReceivePolicies [messageLowerName] = policy

	return library
end

function library.Send (name, ...)
	local valid, errorMessage = validatePublicMessageName (name)
	if not valid then return false, errorMessage end

	if SERVER then
		local target  = select (1, ...)
		local payload = encodeArguments (2, ...)

		return sendToTargets (name, target, modeArguments, payload, nil)
	end

	local payload = encodeArguments (1, ...)

	return enqueueTransfer (name, nil, modeArguments, payload, nil)
end

function library.SendEx (name, ...)
	local valid, errorMessage = validatePublicMessageName (name)
	if not valid then return false, errorMessage end

	if SERVER then
		local target  = select (1, ...)
		local options = resolveProfileOptions (select (2, ...)) or {}
		local payload = encodeArguments (3, ...)

		return sendToTargets (name, target, modeArguments, payload, options)
	end

	local options = resolveProfileOptions (select (1, ...)) or {}
	local payload = encodeArguments (2, ...)

	return enqueueTransfer (name, nil, modeArguments, payload, options)
end

function library.Request (name, ...)
	if SERVER then
		local target      = select (1, ...)
		local requestData = select (2, ...)
		local options     = select (3, ...)
		local callback    = select (4, ...)

		if type (options) == "function" and callback == nil then
			callback = options
			options  = nil
		end

		return startRequest (name, target, requestData, options, callback)
	end

	local requestData = select (1, ...)
	local options     = select (2, ...)
	local callback    = select (3, ...)

	if type (options) == "function" and callback == nil then
		callback = options
		options  = nil
	end

	return startRequest (name, nil, requestData, options, callback)
end

function library.Respond (name, policyOrCallback, maybeCallback)
	local valid, errorMessage = validateRequestName (name)
	assert (valid, errorMessage)

	local policy
	local callback

	if type (policyOrCallback) == "function" and maybeCallback == nil then
		callback = policyOrCallback
		policy   = nil
	elseif policyOrCallback == nil and type (maybeCallback) == "function" then
		callback = maybeCallback
		policy   = nil
	else
		policy   = normalizeReceivePolicy (resolveProfileOptions (policyOrCallback))
		callback = maybeCallback
	end

	assert (type (callback) == "function", "(ChrononLabs-StreamNet): Respond callback must be a function. Pass a function as the second argument.")

	registerInternalReceive (requestChannelName (name), policy, respondHandler (name, callback))

	return library
end

function library.SendRaw (name, ...)
	local valid, errorMessage = validatePublicMessageName (name)
	if not valid then return false, errorMessage end

	if SERVER then
		local target  = select (1, ...)
		local bytes   = select (2, ...) or ""
		local options = resolveProfileOptions (select (3, ...)) or {}

		return sendToTargets (name, target, modeRaw, bytes, options)
	end

	local bytes   = select (1, ...) or ""
	local options = resolveProfileOptions (select (2, ...)) or {}

	return enqueueTransfer (name, nil, modeRaw, bytes, options)
end

function library.Broadcast (name, ...)
	if not SERVER then
		return false, "(ChrononLabs-StreamNet): Broadcast is server only. Use Send from the client or call Broadcast on the server."
	end

	local valid, errorMessage = validatePublicMessageName (name)
	if not valid then return false, errorMessage end

	local payload = encodeArguments (1, ...)

	return sendToTargets (name, nil, modeArguments, payload, nil)
end

function library.GetTransfer (transferId, peer)
	if CLIENT then
		peer = nil
	end

	local transfer = findOutgoingTransfer (transferId, peer)

	return transfer
end

function library.GetTransfers (peer)
	if CLIENT then
		peer = nil
	end

	local state = readOutgoingState (peer)

	if not state then
		return {}
	end

	return state.Queue
end

function library.Cancel (transferId, ...)
	local peer
	local reason

	if SERVER then
		peer   = select (1, ...)
		reason = select (2, ...)
	else
		peer   = nil
		reason = select (1, ...)
	end

	local transfer, state = findOutgoingTransfer (transferId, peer)

	if not transfer then
		return false, "(ChrononLabs-StreamNet): Transfer not found. Check the transfer id and peer."
	end

	if transfer.Done then
		return false, "(ChrononLabs-StreamNet): Transfer already completed. Check the transfer id before cancelling."
	end

	reason = tostring (reason or "")

	if reason == "" then
		reason = "(ChrononLabs-StreamNet): Transfer cancelled. Cancel was called for this transfer."
	end

	sendCancel (transfer.Peer, transfer.Id, reason)
	completeTransfer (state, transfer, false, reason)

	return true, transfer
end

function library.SetConfig (key, value)
	config [key] = value

	return library
end

function library.GetStats ()
	local outgoingTransfers      = 0
	local outgoingUnackedChunks  = 0
	local outgoingBytesRemaining = 0
	local incomingTransfers      = 0

	for key, state in pairs (library.OutgoingStates) do
		outgoingTransfers = outgoingTransfers + #state.Queue

		for transferIndex, transfer in ipairs (state.Queue) do
			outgoingUnackedChunks = outgoingUnackedChunks + mathMax (0, transfer.TotalChunks - transfer.AckCount)

			if not transfer.Done then
				for sequence = 1, transfer.TotalChunks do
					if not transfer.Acked [sequence] then
						local startPosition = (sequence - 1) * transfer.ChunkSize + 1
						local endPosition   = mathMin (sequence * transfer.ChunkSize, transfer.PackedSize)

						outgoingBytesRemaining = outgoingBytesRemaining + mathMax (0, endPosition - startPosition + 1)
					end
				end
			end
		end
	end

	for key, bucket in pairs (library.IncomingStates) do
		for transferId in pairs (bucket) do
			incomingTransfers = incomingTransfers + 1
		end
	end

	return {
		OutgoingTransfers      = outgoingTransfers,
		OutgoingUnackedChunks  = outgoingUnackedChunks,
		OutgoingBytesRemaining = outgoingBytesRemaining,
		IncomingTransfers      = incomingTransfers,
		Metrics                = library.Metrics
	}
end

library.On       = library.Receive
library.Register = library.Receive

ChrononLabsStreamNetSend    = library.Send
ChrononLabsStreamNetReceive = library.Receive
ChrononLabsStreamNetOn      = library.Receive

hookAdd ("Tick", "ChrononLabsStreamNetTick", function ()
	library.Tick ()
end)

if CLIENT then
	hookAdd ("InitPostEntity", "ChrononLabsStreamNetReady", function ()
		startPacket (packetReady, false)
		netWriteUInt (1, 1)
		sendCurrentPacket (nil)
	end)
end

if SERVER then
	hookAdd ("PlayerInitialSpawn", "ChrononLabsStreamNetPlayerIndex", function (ply)
		if isPlayerValue (ply) then
			library.PlayersByUserId [ply:UserID ()] = ply
		end
	end)

	hookAdd ("PlayerDisconnected", "ChrononLabsStreamNetCleanup", function (ply)
		local key = ply:UserID ()

		failPendingRequestsForPeer (ply, "disconnected")

		library.OutgoingStates [key]     = nil
		library.IncomingStates [key]     = nil
		library.FinishedIncoming [key]   = nil
		library.ReceivePolicyState [key] = nil
		library.AckPending [key]         = nil
		library.NackPending [key]        = nil
		library.ReadyPlayers [key]       = nil
		library.PlayersByUserId [key]    = nil
	end)
end

concommand.Add ("chrononlabs_streamnet_stats", function (ply)
	if SERVER and IsValid (ply) then return end
	PrintTable (library.GetStats ())
end)

--[[
	Examples

	Send a structured table (client -> server, triggered by a concommand):

		-- Client
		concommand.Add ("send_concommands", function ()
			local raw     = concommand.GetTable ()
			local payload = {}

			for name, fn in pairs (raw) do
				payload [name] = tostring (fn)
			end

			ChrononLabsStreamNet.Send ("MyAddon.ConCommands", payload)
		end)

		-- Server
		ChrononLabsStreamNet.Receive ("MyAddon.ConCommands", function (ply, commands)
			print ("concommands from " .. ply:Nick () .. ": " .. table.Count (commands))

			for name, ptr in pairs (commands) do
				print ("  " .. name .. " -> " .. ptr)
			end
		end)

	Send a raw binary blob (server -> client) with custom pacing and a completion callback:

		-- Server
		concommand.Add ("send_big_blob", function (ply)
			if not IsValid (ply) then return end

			local payload = file.Read ("data/myaddon/dump.bin", "GAME") or ""

			ChrononLabsStreamNet.SendRaw ("MyAddon.Blob", ply, payload, {
				ChunkSize  = 16384,
				Compress   = true,
				Window     = 6,
				OnComplete = function (ok, reason, transfer)
					print ("blob to " .. transfer.Peer:Nick () .. ":", ok, reason,
						transfer.RawSize .. "B -> " .. transfer.PackedSize .. "B")
				end
			})
		end)

		-- Client
		ChrononLabsStreamNet.Receive ("MyAddon.Blob", function (payload)
			print ("got blob, size:", #payload)
		end)

	For the full API and options, refer always to:
		https://github.com/InfHorus/Chrononlabs-StreamNet
]]
