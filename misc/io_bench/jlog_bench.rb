#!/usr/local/bin/ruby

# for dient(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'fileutils'
require 'higgs/jlog'

count = (ARGV.shift || '10000').to_i
log_size = (ARGV.shift || '16384').to_i
data_file = ARGV.shift || 'jlog_test.dat'

puts "#{$0}: COUNT:#{count}, LOG:#{log_size}"

class JLogWriteTask
  def initialize(path, count, log_size, fsync)
    @path = path
    @count = count
    @log_dat = 0xFF.chr * log_size
    @fsync = fsync
  end

  def open
    log = Higgs::JournalLogger.open(@path, @fsync)
    begin
      yield(log)
    ensure
      log.close
    end
  end
  private :open

  def work
    open{|log|
      @count.times do
	log.write(@log_dat)
      end
    }
  end
end

task_list = [
  [ 'jlog write:',         JLogWriteTask.new(data_file, count, log_size, false) ],
  [ 'jlog write (fsync):', JLogWriteTask.new(data_file, count, log_size, true) ]
]

Benchmark.bm(task_list.map{|n,t| n.length }.max) do |x|
  task_list.each do |name, work|
    x.report(name) { work.work }
  end
end
