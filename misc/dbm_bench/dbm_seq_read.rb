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
conf_path = File.join(File.dirname($0), '.strc')

options = {}
options.update(Higgs::Storage.load_conf(conf_path)) if (File.exist? conf_path)
options[:read_only] = true

Higgs::DBM.open(name, options) {|dbm|
  key_list = dbm.transaction{|tx|
    tx.keys.map{|k| k.to_i }.sort.map{|i| i.to_s }
  }

  Benchmark.bm do |x|
    x.report('dbm seq read:') {
      loop_count.times do |i|
        dbm.transaction{|tx|
          data_count.times do |j|
            k = key_list[(i + j) % key_list.length]
            tx[k]
          end
        }
      end
    }
  end
  print "\n"
}

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
