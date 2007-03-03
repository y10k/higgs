#!/usr/local/bin/ruby

 $: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'

loop_count = (ARGV.shift || '100').to_i
data_count = (ARGV.shift || '10').to_i

name = File.join(File.dirname($0), 'foo')
db = Higgs::DBM.new(name, :read_only => true)

key_list = db.transaction{|tx|
  tx.keys.map{|k| k.to_i }.sort.map{|i| i.to_s }
}

Benchmark.bm do |x|
  x.report('dbm seq read:') {
    loop_count.times do |i|
      db.transaction{|tx|
	data_count.times do |j|
	  k = key_list[(i + j) % key_list.length]
	  tx[k]
	end
      }
    end
  }
end
