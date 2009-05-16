#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs'

Higgs::DBM.open('test') {|dbm|
  dbm.transaction{|tx|
    keys = tx.keys
    if (keys.length > 0) then
      for k in keys
        puts "-"
        puts "key: #{k}"
        puts "value: #{tx[k]}"
        tx.each_property(k) do |name, value|
          case (name)
          when Symbol
            puts "system_property[#{name}]: #{value}"
          when String
            puts "custom_property[#{name}]: #{value}"
          else
            raise "unexpected property name: #{name}"
          end
        end
      end
    else
      tx['foobar'] = 'FB'
      tx.set_property('foobar', 'number', 0)
      tx['baz'] = 'BZ'
      tx.set_property('baz', 'number', 1)
      tx['quux'] = 'QX'
      tx.set_property('quux', 'number', 2)
    end
  }
}

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
