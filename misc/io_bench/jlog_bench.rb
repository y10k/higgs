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
  def initialize(path, count, log_size, fsync, cksum_type)
    @path = path
    @count = count
    @log_dat = 0xFF.chr * log_size
    @fsync = fsync
    @cksum_type = cksum_type
  end

  def open
    log = Higgs::JournalLogger.open(@path, @fsync, @cksum_type)
    begin
      yield(log)
    ensure
      log.close
    end
  end
  private :open

  def call
    open{|log|
      @count.times do
        log.write(@log_dat)
      end
    }
  end
end

task_list = []
cksum_list = [ :SUM16, :MD5, :RMD160, :SHA1, :SHA256, :SHA384, :SHA512 ]
for cksum_type in cksum_list
  task_list << [
    "jlog write [#{cksum_type}]:",
    JLogWriteTask.new(data_file, count, log_size, false, cksum_type)
  ]
end
for cksum_type in cksum_list
  task_list << [
    "jlog write (fsync) [#{cksum_type}]:",
    JLogWriteTask.new(data_file, count, log_size, true, cksum_type)
  ]
end

Benchmark.bm(task_list.map{|n, t| n.length }.max) do |x|
  FileUtils.rm_f(data_file)
  task_list.each do |name, task|
    x.report(name) { task.call }
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
