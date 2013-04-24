-- pack blends into string table.
require'pack'
local bit = require'bit'
-- for debugging
local print = print
local cjson = require'cjson'
local jdecode = cjson.decode
local assert = assert
local band = bit.band
local bor = bit.bor
local rshift = bit.rshift
local lshift = bit.lshift
local spack = string.pack
local sunpack = string.unpack
local tconcat = table.concat
local jencode = cjson.encode
local type = type
local unpack = unpack

local sig_flag = 0x10000000-- bit 28
local meta_flag = 0x20000000-- bit 29
local id_width_in_bits = 20

--- Creates a new ringbuffer stream parser.
-- Maybe used with sync or async io.
-- To parse a chunk call @see parser.continue.
-- @param read_bytes MUST be a function, which takes as single argument
-- a number, nbytes. The function MUST return a string which at most length of nbytes.
-- @param on_meta A function which will be called on every complete meta info.
-- The passed in arguments are: id (number),name/event (string,optional),value (string,optional),bindata(string,optional).
-- @param on_sig A function which will be called on every complate signal chunk.
-- The passed in arguments are: id (number),data (string)
-- @usage local p = streaming.parser(function(nbytes) local b,err = sock:receive(nbytes); if err then error(err) else return b end end); p:continue()
local parser = function(read_bytes, on_meta, on_sig, on_err)
  local bits_0_19 = 0xFFFFF
  
  -- constants for states
  local nothing_read =  0
  local header_read = 1
  local size_read = 2
  local data_read = 3
  
  on_err = on_err or print
  
  return {
    buf = '',
    state = nothing_read,
    chunk = {},
    
    --- Internal helper.
    -- Reads and returns exactly nbytes or returns nil.
    read = function(self,nbytes)
      local nleft = nbytes - #self.buf
      local bytes = self.buf .. (read_bytes(nleft) or '')
      if #bytes == nbytes then
        self.buf = ''
        return bytes
      else
        self.buf = bytes
        return nil
      end
    end,
    
    --- Internal helper.
    -- Parses a chunk (meta info or signal) and calls respective
    -- callback if chunk is complete.
    parse_chunk = function(self)
      if self.state < header_read then
        local bytes = self:read(4)
        if not bytes then
          return nil
        end
        local _,header = sunpack(bytes,'>I')
        local maybe_size = band(0xff,rshift(header,id_width_in_bits))
        self.chunk.is_dynsize = maybe_size == 0
        self.chunk.is_meta = band(header,meta_flag) > 0
        self.chunk.is_sig = band(header,sig_flag) > 0
        self.chunk.id = band(header,bits_0_19)
        if self.chunk.is_dynsize == true then
          self.state = header_read
        else
          self.chunk.size = maybe_size
          self.state = size_read
        end
        --   print(cjson.encode(self.chunk))
      end
      if self.state < size_read then
        local bytes = self:read(4)
        if not bytes then
          return nil
        end
        local _
        _,self.chunk.size = sunpack(bytes,'>I')
        self.state = size_read
      end
      
      if self.state < data_read then
        assert(self.chunk.size < 20000,'STRANGE SIZE '..cjson.encode(self.chunk))
        self.chunk.data = self:read(self.chunk.size)
        if not self.chunk.data then
          return nil
        end
        --                        print('ID',self.chunk.id,'SIZE:',self.chunk.data:byte(1,#self.chunk.data))
      end
      self.state = nothing_read
      return self.chunk
    end,
    
    --- Continues to parse, using the read_bytes function passed in to stream_parser.new.
    -- With async io, this function should be could, when input has become readable.
    -- Parses until either maximum number of chunks @see maxchunks have been parsed or
    -- @see read_bytes runs out of bytes.
    -- @param maxchunks The maximum number of chunks to parse. Nil for no limit.
    continue = function(self,maxchunks)
      local i = 0
      while not maxchunks or i < maxchunks do
        i = i + 1
        local chunk = self:parse_chunk()
        if not chunk then
          return
        end
        if chunk.is_sig and on_sig then
          on_sig(chunk.id, chunk.data,chunk)
        elseif chunk.is_meta and on_meta then
          local pos,meta_type = sunpack(chunk.data,'>I')
          if meta_type == 1 then
            local meta_json = chunk.data:sub(pos)
            local ok,meta = pcall(jdecode,meta_json)
            if ok then
              on_meta(chunk.id,meta,chunk)
            else
              local err = {}
              err[1] = 'Malformed JSON Metainfo:'..meta
              local bytes = meta_json:byte(1,#meta_json)
              local hex = {}
              for i,byte in ipairs(bytes) do
                hex[i] = string.format('%x',byte)
              end
              err[2] = table.concat(hex,' ')
              err[3] = meta_json
              on_err(table.concat(err))
            end
          else
            on_err('Unsupported meta_type:'..meta_type)
          end
        end
      end
    end
  }
end

local max_header_inline_size = 255

local header = function(sigid,typ,size)
  local h = bor(sigid,typ)
  if size <= max_header_inline_size then
    h = bor(h,lshift(size,id_width_in_bits))
    return spack('>I',h)
  else
    return spack('>I>I',size)
  end
end

local meta_type_json = spack('>I',1)

local meta = function(sigid,content)
  content = jencode(content)
  local parts = {
    header(sigid,meta_flag,#content+4),-- 4bytes for meta type
    meta_type_json,
    content
  }
  return tconcat(parts)
end

local signal = function(sigid,content)
  if type(content) ~= 'string' then
    local format = content.format
    local data = content.data
    content = spack(format,unpack(data))
  end
  return header(sigid,sig_flag,#content)..content
end

local connect = function(options)
  local ev = require'ev'
  local cjson = require'cjson'
  local loop = ev.Loop.default
  local socket = require'socket'
  local http = require'socket.http'
  local ltn12 = require'ltn12'
  local sigref_formats = options.signals
  local ip = options.ip
  
  local sigrefs = {}
  for sigref in pairs(sigref_formats) do
    table.insert(sigrefs,sigref)
  end
  
  local O_NONBLOCK = 2048
  local http_url = 'http://'..ip..'/rpc'
  local is_empty = function(t)
    return pairs(t)(t) == nil
  end
  
  local jsonrpc_id = 0
  local jsonrpc = function(method,params)
    assert(http_url,'NO HTTP URL PROVIDED')
    jsonrpc_id = jsonrpc_id + 1
    local req = cjson.encode
    {
      id = jsonrpc_id,
      method = method,
      params = params
    }
    local output = {}
    local ok,err = http.request
    {
      url = http_url,
      method = 'POST',
      headers = {
        ['Content-Type'] = 'application/json',
        ['Content-Length'] = #req
      },
      sink = ltn12.sink.table(output),
      source = ltn12.source.string(req)
    }
    assert(ok,'JSON RPC FAILED')
    local res = cjson.decode(table.concat(output))
    if res.error then
      if type(res) == 'table' and res.error.message then
        error(res.error.message)
      else
        error(cjson.encode(res))
      end
    end
    return res.result
  end
  
  local subscribe = function(stream_id,sigrefs)
    jsonrpc(stream_id..'.subscribe',sigrefs)
  end
  
  local unsubscribe = function(stream_id,sigrefs)
    jsonrpc(stream_id..'.unsubscribe',sigrefs)
  end
  
  local chunksize = 4096
  local data = ''
  
  local success
  
  local sock,err = socket.tcp()
  sock:connect(ip,7411)
  assert(sock,err)
  sock:settimeout(0)
  
  local last
  local read_ringbuffer = function(nbytes)
    local data,err,part = sock:receive(nbytes)
    last = data or part
    if data then
      return data
    elseif err == 'timeout' then
      return part
    else
      if not success then
        if options.on_error then
          options.on_all_unsubscribed('stream socket error:' .. err)
        end
      end
    end
  end
  local meta_type_json = 1
  local stream_id
  local subscribed = 0
  local sigids = {}
  local on_all_unsubscribed
  local parser = parser(
    read_ringbuffer,
    function(id,data_json)
        local data_json = cjson.decode(data)
        if id == 0 and data_json.method == 'init' and #sigrefs > 0 then
          assert(type(data_json.params.streamId) == 'string')
          assert(not stream_id)
          stream_id = data_json.params.streamId
          subscribe(stream_id,sigrefs)
        elseif data_json.method == 'subscribe' then
          assert(not sigids[id])
          local sigref = data_json.params[1]
          sigids[id] = sigref
          subscribed = subscribed + 1
          if options.on_subscribe then
            options.on_subscribe(sigref,subscribed == #sigrefs)
          end
          if sigref_formats[sigref].on_subscribe then
            sigref_formats[sigref].on_subscribe(subscribed == #sigrefs)
          end
          if subscribed == #sigrefs and options.on_all_subscribed then
            options.on_all_subscribed()
          end
        elseif data_json.method == 'unsubscribe' then
          local sigref = sigids[id]
          assert(sigref)
          subscribed = subscribed - 1
          if subscribed == 0 and options.on_all_unsubscribed then
            options.on_all_unsubscribed()
            if on_all_unsubscribed then
              on_all_unsubscribed()
            end
          end
          if options.on_unsubscribe then
            options.on_unsubscribe(sigref,subscribed == 0)
          end
          if sigref_formats[sigref].on_unsubscribe then
            sigref_formats[sigref].on_unsubscribe(subscribed == #sigrefs)
          end
          sigids[id] = nil
        end
        if options.on_meta then
          options.on_meta(sigids[id],data_json)
        end
        local sigref = sigids[id]
        if sigref and sigref_formats[sigref].on_meta then
          sigref_formats[sigref].on_meta(data_json)
        end
    end,
    function(id,data)
      local sigref = sigids[id]
      local format = sigref_formats[sigref].format
      local decoded
      if format then
        decoded = {data:unpack(format)}
        table.remove(decoded,1)
      end
      if options.on_signal then
        options.on_signal(sigref,decoded,data)
      end
      if sigref_formats[sigref].on_signal then
        sigref_formats[sigref].on_signal(decoded,data)
      end
    end)
  
  
  local ios = {}
  
  table.insert(
    ios,ev.IO.new(
      function(loop,io)
        if success then
          io:stop(loop)
        end
        local ok,err = pcall(parser.continue,parser)
        if not ok then
          io:stop(loop)
          print('ERROR:',err)
          if callback.on_error then
            callback.on_error(err)
          end
        end
      end
  ,sock:getfd(),ev.READ))
  
  local SIGHUP = 1
  local SIGINT = 2
  local SIGKILL = 9
  local SIGTERM = 15
  local quit =function()
    for _,io in ipairs(ios) do
      io:stop(loop)
    end
    loop:unloop()
  end
  table.insert(ios,ev.Signal.new(quit,SIGHUP))
  table.insert(ios,ev.Signal.new(quit,SIGINT))
  table.insert(ios,ev.Signal.new(quit,SIGKILL))
  table.insert(ios,ev.Signal.new(quit,SIGTERM))
  
  for _,io in ipairs(ios) do
    io:start(loop)
  end
  local close = function(_,wait_unsubscribe)
    if wait_unsubscribe then
      on_all_unsubscribed = function()
        sock:close()
      end
      unsubscribe(stream_id,sigrefs)
    else
      sock:close()
    end
  end
  return {
    close = close,
    loop = function()
      loop:loop()
    end
  }
end


return {
  parser = parser,
  meta = meta,
  signal = signal,
  connect = connect
}
