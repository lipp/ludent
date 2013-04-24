local streaming = require'streaming'
local cjson = require'cjson'


it('generates correct meta data',function()
    local data = io.open('example_data/pub_unsub.dump'):read('*a')
    local ref_header_and_metatype = data:sub(1,0x8)
    local version = {method='version',params={'1.0'}}
    local version_meta = streaming.meta(0,version)
    assert.is_equal(#version_meta,#cjson.encode(version) + 8)
    assert.is_equal(data:sub(1,8),version_meta:sub(1,8))
  end)

local fake_data

it('can generate some more elaborate fake_data',function()
    local meta_data = streaming.meta(3,{some_data='hello'})
    local sig_data = streaming.signal(4,'kkkk')
    local sig_data_float = streaming.signal(16,{format='>f>f',data = {1,423}})
    fake_data = table.concat({
        meta_data,
        sig_data,
        meta_data,
        sig_data_float,
        sig_data,
        sig_data
    })
  end)

local generate_parse_test = function(read_step)
  local read_index
  local read_fake_data = function(n)
    local step = math.min(read_step,n)
    if read_index <= #fake_data then
      local chunk = fake_data:sub(read_index,read_index+step-1)
      read_index = read_index+step
      return chunk
    else
      return nil
    end
  end
  
  it('generated fake data can be parsed in read_step='..read_step,function()
      read_index = 1
      local on_meta = spy.new(function(sigid,meta)
          assert.is_equal(sigid,3)
          assert.is_same(meta,{some_data='hello'})
        end)
      
      local on_sig = spy.new(function(sigid,sigdata)
          if sigid == 4 then
            assert.is_equal(sigdata,'kkkk')
          elseif sigid == 16 then
            local _,f1,f2 = sigdata:unpack('>f>f')
            assert.is_equal(f1,1)
            assert.is_equal(f2,423)
          else
            assert.is_falsy('should never happen')
          end
        end)
      
      local parser = streaming.parser(read_fake_data,on_meta,on_sig)
      repeat
        parser:continue()
      until read_index > #fake_data
      assert.spy(on_meta).was.called(2)
      assert.spy(on_sig).was.called(4)
    end)
end

for i=1,30 do
  generate_parse_test(i)
end
