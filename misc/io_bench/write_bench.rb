#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'benchmark'

segment_count = (ARGV.shift || '10000').to_i
segment_size = (ARGV.shift || '16384').to_i
chunk_count = (ARGV.shift || '10').to_i
data_file = ARGV.shift || 'io_test.dat'

puts "#{$0}: COUNT:#{segment_count}, SEGMENT:#{segment_size}, CHUNK:#{chunk_count}"

class WriteTask
  def initialize(path, count, size, chunk, fsync)
    @path = path
    @count = count
    @chunk = chunk
    @data = 0xFF.chr * size
    @fsync = fsync
    srand(0)                    # reset for rand
  end

  def open
    begin
      w = File.open(@path, File::WRONLY | File::CREAT | File::EXCL)
    rescue Errno::EEXIST
      w = File.open(@path, File::WRONLY)
    end

    begin
      w.binmode
      yield(w)
    ensure
      w.close
    end
  end
  private :open

  def io_sync(io)
    if (@fsync) then
      io.fsync
    else
      io.sync
    end
  end
  private :io_sync
end

class SequentialWriteTask < WriteTask
  def work
    open{|w|
      @count.times do |i|
        w.write(@data)
        if (i % @chunk == 0) then
          io_sync(w)
        end
      end
      io_sync(w)
    }
  end
end

class RandomWriteTask < WriteTask
  def work
    open{|w|
      @count.times do |i|
        pos = rand(@count)
        w.seek(@data.size * pos)
        w.write(@data)
        if (i % @chunk == 0) then
          io_sync(w)
        end
      end
      io_sync(w)
    }
  end
end

class SortedRandomWriteTask < WriteTask
  def work
    open{|w|
      write_list = []
      @count.times do |i|
        pos = rand(@count)
        write_list << pos
        if (i % @chunk == 0) then
          write_list.sort!
          for pos in write_list
            w.seek(@data.size * pos)
            w.write(@data)
          end
          write_list.clear
          io_sync(w)
        end
      end

      write_list.sort!
      for pos in write_list
        w.seek(@data.size * pos)
        w.write(@data)
      end
      write_list.clear
      io_sync(w)
    }
  end
end

task_list = [
  [ 'seq write:',         SequentialWriteTask.new(data_file, segment_count, segment_size, chunk_count, false) ],
  [ 'seq write (fsync):', SequentialWriteTask.new(data_file, segment_count, segment_size, chunk_count, true) ],
  [ 'rnd write:',         RandomWriteTask.new(data_file, segment_count, segment_size, chunk_count, false) ],
  [ 'rnd write (fsync):', RandomWriteTask.new(data_file, segment_count, segment_size, chunk_count, true) ],
  [ 'sorted rnd write:',  SortedRandomWriteTask.new(data_file, segment_count, segment_size, chunk_count, false) ],
  [ 'sorted rnd write (fsync):', SortedRandomWriteTask.new(data_file, segment_count, segment_size, chunk_count, true) ]
]

Benchmark.bm(task_list.map{|n,t| n.length }.max) do |x|
  task_list.each do |name, work|
    x.report(name) { work.work }
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
