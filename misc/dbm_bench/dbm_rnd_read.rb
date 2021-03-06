#!/usr/local/bin/ruby

$:.unshift File.join(File.dirname($0), '..', '..', 'lib')

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
  srand(0)
  key_list = dbm.transaction{|tx| tx.keys }

  Benchmark.bm do |x|
    x.report('dbm rnd read:') {
      loop_count.times do
        dbm.transaction{|tx|
          data_count.times do
            k = key_list[rand(key_list.length)]
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
