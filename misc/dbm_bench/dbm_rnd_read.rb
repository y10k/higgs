#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

 $: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'

loop_count = (ARGV.shift || '100').to_i
data_count = (ARGV.shift || '10').to_i
puts "#{$0}: LOOP:#{loop_count}, DATA:#{data_count}"

name = File.join(File.dirname($0), 'foo')
db = Higgs::DBM.new(name, :read_only => true)

srand(0)
key_list = db.transaction{|tx| tx.keys }

Benchmark.bm do |x|
  x.report('dbm rnd read:') {
    loop_count.times do
      db.transaction{|tx|
	data_count.times do
	  k = key_list[rand(key_list.length)]
	  tx[k]
	end
      }
    end
  }
end
print "\n"