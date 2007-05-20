#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

 $: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'
require 'stopts'

loop_count = (ARGV.shift || '100').to_i
data_count = (ARGV.shift || '10').to_i
max_dat_len = (ARGV.shift || '32768').to_i
puts "#{$0}: LOOP:#{loop_count}, DATA:#{data_count}, MAX_DAT_LEN:#{max_dat_len}"

options = get_storage_options
name = File.join(File.dirname($0), 'foo')

Higgs::DBM.open(name, options) {|dbm|
  srand(0)
  key_list = dbm.transaction{|tx| tx.keys }

  Benchmark.bm do |x|
    x.report('dbm rnd update:') {
      loop_count.times do
        dbm.transaction{|tx|
          data_count.times do
            k = key_list[rand(key_list.length)]
            tx[k] = 0xFF.chr * (rand(max_dat_len))
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
