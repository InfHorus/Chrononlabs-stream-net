--[[
	ChrononLabs-Stream-Net
	Single file streamed networking layer for Garry's Mod.

	Written by Enumerator (@makeconstructor) - 20/05/2026

	Main usage:
		ChrononLabsStreamNet.Receive (name, callback)
		ChrononLabsStreamNet.Send    (name, [target], ...)   -- target is server-side only
		ChrononLabsStreamNet.Broadcast(name, ...)            -- server only

	For the full API (SendEx, SendRaw, options, stats, etc...), see the README:
		https://github.com/InfHorus/Chrononlabs-StreamNet
]]

if SERVER then
	AddCSLuaFile ()
end

ChrononLabsStreamNet  = ChrononLabsStreamNet or {}

local library         = ChrononLabsStreamNet
local channelName     = "ChrononLabsStreamNet"
local protocolVersion = 1

local packetData     = 1
local packetAck      = 2
local packetNack     = 3
local packetComplete = 4
local packetCancel   = 5
local packetReady    = 6

local modeArguments = 1
local modeRaw       = 2

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
local mathHuge     = math.huge
local mathMax      = math.max
local mathMin      = math.min
local stringByte   = string.byte
local stringChar   = string.char
local stringFormat = string.format
local stringLower  = string.lower
local stringSub    = string.sub
local tableConcat  = table.concat
local tableInsert  = table.insert
local tableRemove  = table.remove
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
config.CompressAt                      = config.CompressAt or 1024
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
config.QueueUntilClientReady           = config.QueueUntilClientReady or false
config.Debug                           = config.Debug or false

channelName = config.ChannelName

library.Handlers         = library.Handlers or {}
library.OutgoingStates   = library.OutgoingStates or {}
library.IncomingStates   = library.IncomingStates or {}
library.FinishedIncoming = library.FinishedIncoming or {}
library.AckPending       = library.AckPending or {}
library.NackPending      = library.NackPending or {}
library.ReadyPlayers     = library.ReadyPlayers or {}
library.NextTransferId   = library.NextTransferId or math.random (1, 2147483000)
library.Metrics          = library.Metrics or {
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

local function isPlayerValue (value)
	return IsValid (value) and value.IsPlayer and value:IsPlayer ()
end

local function nextTransferId ()
	library.NextTransferId = library.NextTransferId + 1
	if library.NextTransferId >= 4294967295 then
		library.NextTransferId = 1
	end

	return library.NextTransferId
end

local function crc (data)
	return tostring (utilCRC (data or ""))
end

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

local function numberToString (numberValue)
	if numberValue ~= numberValue then return "nan" end
	if numberValue == mathHuge then return "inf" end
	if numberValue == -mathHuge then return "ninf" end
	return stringFormat ("%.17g", numberValue)
end

local function stringToNumber (data)
	if data == "nan" then return 0 / 0 end
	if data == "inf" then return mathHuge end
	if data == "ninf" then return -mathHuge end
	return tonumber (data) or 0
end

local function writeNumberText (output, numberValue)
	writeString (output, numberToString (numberValue or 0))
end

local function readUnsigned8 (reader)
	if reader.Position > reader.Length then
		error ("ChrononLabsStreamNet decode reached end of stream")
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
		error ("ChrononLabsStreamNet decode string length is invalid")
	end

	if reader.Position + length - 1 > reader.Length then
		error ("ChrononLabsStreamNet decode string exceeds stream")
	end

	local data      = stringSub (reader.Data, reader.Position, reader.Position + length - 1)
	reader.Position = reader.Position + length

	return data
end

local function readNumberText (reader)
	return stringToNumber (readString (reader))
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
		if value == mathFloor (value) and value >= -2147483648 and value <= 2147483647 then
			writeUnsigned8 (output, tagInt32)
			writeUnsigned32 (output, value)
		else
			writeUnsigned8 (output, tagNumber)
			writeNumberText (output, value)
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

		if IsValid (value) then
			writeUnsigned32 (output, value:EntIndex ())
		else
			writeUnsigned32 (output, 0)
		end

		return
	end

	if isvector and isvector (value) then
		writeUnsigned8 (output, tagVector)
		writeNumberText (output, value.x)
		writeNumberText (output, value.y)
		writeNumberText (output, value.z)
		return
	end

	if isangle and isangle (value) then
		writeUnsigned8 (output, tagAngle)
		writeNumberText (output, value.p)
		writeNumberText (output, value.y)
		writeNumberText (output, value.r)
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
			error ("ChrononLabsStreamNet serializer table depth limit exceeded")
		end

		if seen [value] then
			error ("ChrononLabsStreamNet serializer cyclic table")
		end

		seen [value] = true

		local pairCount = 0

		for key in pairs (value) do
			pairCount = pairCount + 1

			if pairCount > config.MaximumTablePairs then
				seen [value] = nil
				error ("ChrononLabsStreamNet serializer table pair limit exceeded")
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

	error ("ChrononLabsStreamNet serializer unsupported type " .. valueType)
end

readValue = function (reader, depth)
	if depth > config.MaximumTableDepth then
		error ("ChrononLabsStreamNet decode table depth limit exceeded")
	end

	local tag = readUnsigned8 (reader)

	if tag == tagNil then return nil end
	if tag == tagFalse then return false end
	if tag == tagTrue then return true end
	if tag == tagInt32 then return readSigned32 (reader) end
	if tag == tagNumber then return readNumberText (reader) end
	if tag == tagString then return readString (reader) end

	if tag == tagVector then
		return Vector (readNumberText (reader), readNumberText (reader), readNumberText (reader))
	end

	if tag == tagAngle then
		return Angle (readNumberText (reader), readNumberText (reader), readNumberText (reader))
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

		if Entity then
			return Entity (entityIndex)
		end

		return NULL
	end

	if tag == tagTable then
		local pairCount = readUnsigned32 (reader)

		if pairCount > config.MaximumTablePairs then
			error ("ChrononLabsStreamNet decode table pair limit exceeded")
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

	error ("ChrononLabsStreamNet decode unknown tag " .. tostring (tag))
end

local function encodeArguments (startIndex, ...)
	local argumentCount = select ("#", ...) - startIndex + 1

	if argumentCount < 0 then
		argumentCount = 0
	end

	local output = {}
	writeUnsigned8 (output, 1)
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

	if version ~= 1 then
		error ("ChrononLabsStreamNet decode serializer version mismatch")
	end

	local argumentCount = readUnsigned16 (reader)
	local arguments     = {}

	for argumentIndex = 1, argumentCount do
		arguments [argumentIndex] = readValue (reader, 0)
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

	for _, ply in playerIterator () do
		if ply:UserID () == key then
			return ply
		end
	end

	return nil
end

local function getOutgoingState (peer)
	local key = peerKey (peer)
	if not key then return nil end

	local state = library.OutgoingStates [key]

	if not state then
		state = {
			Key      = key,
			Peer     = peer,
			Queue    = {},
			ById     = {},
			Budget   = config.BurstBytes,
			LastTick = now ()
		}

		library.OutgoingStates [key] = state
	else
		state.Peer = peer
	end

	return state
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

local function safeChunkSize (name, wantedChunkSize)
	wantedChunkSize = tonumber (wantedChunkSize) or config.ChunkSize

	local overhead         = 160 + #tostring (name or "")
	local maximumChunkSize = clamp (config.MaximumNetMessageBytes - overhead, 512, 60000)

	return clamp (mathFloor (wantedChunkSize), 512, maximumChunkSize)
end

local function makeTransfer (name, peer, payloadMode, payload, options)
	options = options or {}
	name    = tostring (name or "")

	if name == "" then return false, "empty message name" end
	if #name > 128 then return false, "message name too long" end
	if type (payload) ~= "string" then return false, "payload must be a string" end
	if #payload > config.MaximumPayloadBytes then return false, "payload too large" end

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
		return false, "packed payload too large"
	end

	local chunkSize   = safeChunkSize (name, options.ChunkSize)
	local totalChunks = mathMax (1, mathCeil (#payload / chunkSize))

	local transfer = {
		Id             = nextTransferId (),
		Name           = name,
		LowerName      = lowerName (name),
		Mode           = payloadMode,
		Peer           = peer,
		Data           = payload,
		RawSize        = rawSize,
		PackedSize     = #payload,
		Compressed     = compressed,
		Checksum       = crc (payload),
		ChunkSize      = chunkSize,
		TotalChunks    = totalChunks,
		NextSequence   = 1,
		Sent           = {},
		Retries        = {},
		Acked          = {},
		InFlight       = {},
		InFlightCount  = 0,
		AckCount       = 0,
		NackQueue      = {},
		NackSeen       = {},
		NackHead       = 1,
		CreatedAt      = now (),
		LastProgress   = now (),
		RetryInterval  = tonumber (options.RetryInterval) or config.RetryInterval,
		Timeout        = tonumber (options.Timeout) or config.Timeout,
		MaximumRetries = tonumber (options.MaximumRetries) or config.MaximumRetries,
		Window         = tonumber (options.Window) or config.Window,
		ReliableData   = options.ReliableData == true,
		Callback       = options.OnComplete or options.onComplete
	}

	return transfer
end

local function enqueueTransfer (name, target, payloadMode, payload, options)
	local state = getOutgoingState (target)
	if not state then return false, "invalid target" end

	local transfer, errorMessage = makeTransfer (name, target, payloadMode, payload, options)

	if not transfer then
		return false, errorMessage
	end

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

	if isPlayerValue (target) then
		local id, result = enqueueTransfer (name, target, payloadMode, payload, options)
		if not id then return false, result end
		return id
	end

	if target == nil or target == true then
		local ids      = {}
		local anySent  = false
		local lastError = nil

		for _, ply in playerIterator () do
			local id, result = enqueueTransfer (name, ply, payloadMode, payload, options)

			if id then
				ids [#ids + 1] = id
				anySent        = true
			else
				lastError = result
			end
		end

		if not anySent then
			return false, lastError or "no valid targets"
		end

		if #ids == 1 then
			return ids [1]
		end

		return ids
	end

	if type (target) == "table" then
		local ids      = {}
		local anySent  = false
		local lastError = nil

		for key, value in pairs (target) do
			local ply = nil

			if isPlayerValue (value) then
				ply = value
			elseif isPlayerValue (key) then
				ply = key
			end

			if ply then
				local id, result = enqueueTransfer (name, ply, payloadMode, payload, options)

				if id then
					ids [#ids + 1] = id
					anySent        = true
				else
					lastError = result
				end
			end
		end

		if not anySent then
			return false, lastError or "no valid targets"
		end

		if #ids == 1 then
			return ids [1]
		end

		return ids
	end

	return false, "no valid targets"
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
	netWriteString (tostring (reason or "cancel"))
	sendCurrentPacket (peer)
end

local function sendComplete (peer, transferId, ok, reason)
	startPacket (packetComplete, false)
	writeNetUnsigned32 (transferId)
	netWriteBool (ok == true)
	netWriteString (tostring (reason or ""))
	sendCurrentPacket (peer)
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

	if transfer.Callback then
		local callbackOk, callbackError = pcall (transfer.Callback, ok, reason or "", transfer)

		if not callbackOk then
			ErrorNoHalt ("(ChrononLabs-StreamNet) OnComplete error: " .. tostring (callbackError) .. "\n")
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
		return
	end

	if currentTime - transfer.LastProgress > transfer.Timeout then
		sendCancel (transfer.Peer, transfer.Id, "sender timeout")
		completeTransfer (state, transfer, false, "timeout")
		return
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
			sendCancel (transfer.Peer, transfer.Id, "maximum retries reached")
			completeTransfer (state, transfer, false, "maximum retries reached")
			return
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
			completeTransfer (state, transfer, false, "send failed")
			return
		end

		state.Budget = mathMax (0, state.Budget - usedBytes)
		sentPackets  = sentPackets + 1
	end
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

			local transferIndex = 1

			while transferIndex <= #state.Queue do
				local transfer = state.Queue [transferIndex]

				if transfer.Done then
					tableRemove (state.Queue, transferIndex)
				else
					pumpTransfer (state, transfer, currentTime)
					transferIndex = transferIndex + 1
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
		failIncoming (peer, bucket, incoming, "assembled size mismatch")
		return
	end

	if crc (packedPayload) ~= incoming.Checksum then
		failIncoming (peer, bucket, incoming, "assembled checksum mismatch")
		return
	end

	local payload = packedPayload

	if incoming.Compressed then
		if not utilDecompress then
			failIncoming (peer, bucket, incoming, "decompressor unavailable")
			return
		end

		local decompressOk, decompressedPayload = pcall (utilDecompress, packedPayload, incoming.RawSize)

		if not decompressOk or type (decompressedPayload) ~= "string" then
			failIncoming (peer, bucket, incoming, "decompression failed")
			return
		end

		payload = decompressedPayload
	end

	if #payload ~= incoming.RawSize then
		failIncoming (peer, bucket, incoming, "raw size mismatch")
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
				failIncoming (peer, bucket, incoming, "decode failed")
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
				ErrorNoHalt ("(ChrononLabs-StreamNet) handler error for " .. incoming.Name .. ": " .. tostring (callbackError) .. "\n")
			end
		else
			local callbackOk, callbackError

			if SERVER then
				callbackOk, callbackError = pcall (callback, peer, tableUnpack (arguments, 1, argumentCount))
			else
				callbackOk, callbackError = pcall (callback, tableUnpack (arguments, 1, argumentCount))
			end

			if not callbackOk then
				ErrorNoHalt ("(ChrononLabs-StreamNet) handler error for " .. incoming.Name .. ": " .. tostring (callbackError) .. "\n")
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
	if totalChunks < 1 or totalChunks > 1048576 then return end
	if sequence < 1 or sequence > totalChunks then return end
	if #chunk ~= chunkLength then return end

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
			sendCancel (peer, transferId, finished.Reason ~= "" and finished.Reason or "transfer failed")
			finished.LastControlSent = currentTime
		end

		return
	end

	local bucket = library.IncomingStates [key]

	if not bucket then
		bucket                       = {}
		library.IncomingStates [key] = bucket
	end

	local incoming = bucket [transferId]

	if not incoming then
		if countTable (bucket) >= config.MaximumIncomingTransfersPerPeer then
			sendCancel (peer, transferId, "too many incoming transfers")
			return
		end

		incoming = {
			Id            = transferId,
			Mode          = payloadMode,
			Name          = name,
			LowerName     = lowerName (name),
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
	else
		if incoming.Mode ~= payloadMode or incoming.Name ~= name or incoming.Compressed ~= compressed or incoming.RawSize ~= rawSize or incoming.PackedSize ~= packedSize or incoming.TotalChunks ~= totalChunks or incoming.Checksum ~= fullChecksum then
			failIncoming (peer, bucket, incoming, "metadata mismatch")
			return
		end
	end

	incoming.UpdatedAt = currentTime

	if not incoming.Chunks [sequence] then
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
		completeTransfer (state, transfer, false, reason ~= "" and reason or "remote cancel")
	end

	local bucket = getIncomingBucket (peer)

	if bucket then
		local incoming = bucket [transferId]

		if incoming then
			rememberFinishedIncoming (peer, incoming, false, reason ~= "" and reason or "remote cancel")
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
					failIncoming (peer, bucket, incoming, "incoming timeout")
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

	if currentTime >= nextControlFlush then
		flushControlRoot (library.AckPending, packetAck, config.AckBatch)
		flushControlRoot (library.NackPending, packetNack, config.NackBatch)
		nextControlFlush = currentTime + config.AckInterval
	end
end

function library.Receive (name, callback)
	assert (type (name) == "string", "ChrononLabsStreamNet.Receive name must be a string")
	assert (type (callback) == "function", "ChrononLabsStreamNet.Receive callback must be a function")

	library.Handlers [lowerName (name)] = callback

	return library
end

function library.Send (name, ...)
	if SERVER then
		local target  = select (1, ...)
		local payload = encodeArguments (2, ...)

		return sendToTargets (name, target, modeArguments, payload, nil)
	end

	local payload = encodeArguments (1, ...)

	return enqueueTransfer (name, nil, modeArguments, payload, nil)
end

function library.SendEx (name, ...)
	if SERVER then
		local target  = select (1, ...)
		local options = select (2, ...) or {}
		local payload = encodeArguments (3, ...)

		return sendToTargets (name, target, modeArguments, payload, options)
	end

	local options = select (1, ...) or {}
	local payload = encodeArguments (2, ...)

	return enqueueTransfer (name, nil, modeArguments, payload, options)
end

function library.SendRaw (name, ...)
	if SERVER then
		local target  = select (1, ...)
		local bytes   = select (2, ...) or ""
		local options = select (3, ...) or {}

		return sendToTargets (name, target, modeRaw, bytes, options)
	end

	local bytes   = select (1, ...) or ""
	local options = select (2, ...) or {}

	return enqueueTransfer (name, nil, modeRaw, bytes, options)
end

function library.Broadcast (name, ...)
	if not SERVER then
		return false, "Broadcast is server only"
	end

	local payload = encodeArguments (1, ...)

	return sendToTargets (name, nil, modeArguments, payload, nil)
end

function library.SetConfig (key, value)
	config [key] = value

	return library
end

function library.GetStats ()
	local outgoingTransfers     = 0
	local outgoingUnackedChunks = 0
	local incomingTransfers     = 0

	for key, state in pairs (library.OutgoingStates) do
		outgoingTransfers = outgoingTransfers + #state.Queue

		for transferIndex, transfer in ipairs (state.Queue) do
			outgoingUnackedChunks = outgoingUnackedChunks + mathMax (0, transfer.TotalChunks - transfer.AckCount)
		end
	end

	for key, bucket in pairs (library.IncomingStates) do
		for transferId in pairs (bucket) do
			incomingTransfers = incomingTransfers + 1
		end
	end

	return {
		OutgoingTransfers     = outgoingTransfers,
		OutgoingUnackedChunks = outgoingUnackedChunks,
		IncomingTransfers     = incomingTransfers,
		Metrics               = library.Metrics
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
	hookAdd ("PlayerDisconnected", "ChrononLabsStreamNetCleanup", function (ply)
		local key = ply:UserID ()

		library.OutgoingStates [key]   = nil
		library.IncomingStates [key]   = nil
		library.FinishedIncoming [key] = nil
		library.AckPending [key]       = nil
		library.NackPending [key]      = nil
		library.ReadyPlayers [key]     = nil
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
