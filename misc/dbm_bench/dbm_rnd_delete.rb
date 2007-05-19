#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/dbm'
require 'logger'
require 'yaml'

loop_count = (ARGV.shift || '100').to_i
data_count = (ARGV.shift || '10').to_i
puts "#{$0}: LOOP:#{loop_count}, DATA:#{data_count}"

options = {}
if (File.exist? '.strc') then
  for name, value in YAML.load(IO.read('.strc'))
    options[name.to_sym] = value
  end
end
options[:logger] = proc{|path|
  logger = Logger.new(path, 1)
  logger.level = case (options.delete(:log_level))
		 when 'debug'
		   Logger::DEBUG
		 when 'info'
		   Logger::INFO
		 when 'warn'
		   Logger::WARN
		 when 'error'
		   Logger::ERROR
		 when 'fatal'
		   Logger::FATAL
		 else
		   Logger::INFO
		 end
  logger
}

name = File.join(File.dirname($0), 'foo')
Higgs::DBM.open(name, options) {|dbm|
  srand(0)
  key_list = dbm.transaction{|tx| tx.keys }

  Benchmark.bm do |x|
    x.report('dbm rnd delete:') {
      loop_count.times do
        dbm.transaction{|tx|
          data_count.times do
            k = key_list[rand(key_list.length)]
            tx.delete(k)
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
