#!/usr/local/bin/ruby

$:.unshift File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'

loop_count = (ARGV.shift || '100').to_i
data_count = (ARGV.shift || '10').to_i
max_dat_len = (ARGV.shift || '32768').to_i
puts "#{$0}: LOOP:#{loop_count}, DATA:#{data_count}, MAX_DAT_LEN:#{max_dat_len}"

name = File.join(File.dirname($0), 'foo')
conf_path = File.join(File.dirname($0), '.strc')

options = {}
options.update(Higgs::Storage.load_conf(conf_path)) if (File.exist? conf_path)

Higgs::DBM.open(name, options) {|dbm|
  srand(0)

  Benchmark.bm do |x|
    x.report('dbm seq write:') {
      loop_count.times do |i|
        dbm.transaction{|tx|
          data_count.times do |j|
            k = (i * data_count + j).to_s
            d = 0xFF.chr * (rand(max_dat_len))
            tx[k] = d
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
