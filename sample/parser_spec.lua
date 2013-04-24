local streaming = require'streaming'

it('can parse "pub_subsub.dump" without error',function()
    assert.has_no_error(function()
        local data = io.open('example_data/pub_unsub.dump')
        local read_bytes = function(n)
          return data:read(n)
        end
        local parser = streaming.parser(read_bytes)
        parser:continue()
      end)
  end)
